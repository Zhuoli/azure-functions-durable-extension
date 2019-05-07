﻿// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Common;
using DurableTask.Core.Exceptions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    /// <summary>
    /// Implements the entity scheduler as a looping orchestration.
    /// There is one such orchestration per entity.
    /// The orchestration terminates if the entity is deleted and idle.
    /// The orchestration calls ContinueAsNew when it is idle, but not deleted.
    /// </summary>
    internal class TaskEntityShim : TaskCommonShim
    {
        private readonly DurableEntityContext context;

        private readonly TaskCompletionSource<object> doneProcessingMessages
            = new TaskCompletionSource<object>();

        // a batch always consists of a (possibly empty) sequence of operations
        // followed by zero or one lock request
        private readonly List<RequestMessage> operationBatch = new List<RequestMessage>();
        private RequestMessage lockRequest = null;

        public TaskEntityShim(DurableTaskExtension config, string schedulerId)
            : base(config)
        {
            this.SchedulerId = schedulerId;
            this.EntityId = EntityId.GetEntityIdFromSchedulerId(schedulerId);
            this.context = new DurableEntityContext(config, this.EntityId, this);
        }

        public override DurableCommonContext Context => this.context;

        public string SchedulerId { get; private set; }

        public EntityId EntityId { get; private set; }

        public int NumberEventsToReceive { get; set; }
    
        internal List<RequestMessage> OperationBatch => this.operationBatch;

        public void AddOperationToBatch(RequestMessage operationMessage)
        {
            this.operationBatch.Add(operationMessage);
        }

        public void AddLockRequestToBatch(RequestMessage lockRequest)
        {
            this.lockRequest = lockRequest;
        }

        public override RegisteredFunctionInfo GetFunctionInfo()
        {
            FunctionName entityFunction = new FunctionName(this.Context.FunctionName);
            return this.Config.GetEntityInfo(entityFunction);
        }

        public override string GetStatus()
        {
            // We assemble a status object that compactly describes the current
            // state of the entity scheduler. It excludes all potentially large data
            // such as the entity state or the contents of the queue, so it always
            // has reasonable latency.

            EntityCurrentOperationStatus opStatus = null;
            if (this.context.CurrentOperation != null)
            {
                opStatus = new EntityCurrentOperationStatus()
                {
                    Operation = this.context.CurrentOperation.Operation,
                    Id = this.context.CurrentOperation.Id,
                    ParentInstanceId = this.context.CurrentOperation.ParentInstanceId,
                    StartTime = this.context.CurrentOperationStartTime,
                };
            }

            return MessagePayloadDataConverter.Default.Serialize(new EntityStatus()
            {
                EntityExists = this.context.State.EntityExists,
                QueueSize = this.context.State.Queue?.Count ?? 0,
                LockedBy = this.context.State.LockedBy,
                CurrentOperation = opStatus,
            });
        }

        public override void RaiseEvent(OrchestrationContext unused, string eventName, string serializedEventData)
        {
            // no-op: the events were already processed outside of the DTFx context
            if (--this.NumberEventsToReceive == 0)
            {
                // signal the main orchestration thread that it can now safely terminate.
                this.doneProcessingMessages.SetResult(null);
            }
        }

        internal void Rehydrate(string serializedInput)
        {
            if (this.GetFunctionInfo().IsOutOfProc)
            {
                throw new NotImplementedException("out-of-proc entity support is not implemented yet");
            }

            if (serializedInput == null)
            {
                // this instance was automatically started by DTFx
                this.context.State = new SchedulerState();
            }
            else
            {
                // a previous incarnation of this instance called continueAsNew
                this.context.State = JsonConvert.DeserializeObject<SchedulerState>(serializedInput, MessagePayloadDataConverter.MessageSettings);
            }

            if (!this.context.State.EntityExists)
            {
                this.context.AddDeferredTask(() => this.Config.LifeCycleNotificationHelper.OrchestratorStartingAsync(
                    this.context.HubName,
                    this.context.Name,
                    this.context.InstanceId,
                    this.context.IsReplaying));
            }
        }

        public override async Task<string> Execute(OrchestrationContext innerContext, string serializedInput)
        {
            if (this.operationBatch.Count == 0 && this.lockRequest == null)
            {
                // we are idle after a ContinueAsNew - the batch is empty.
                // Wait for more messages to get here (via extended sessions)
                await this.doneProcessingMessages.Task;
            }

            // Send all buffered outgoing messages
            this.context.SendOutbox(innerContext);

            if (this.NumberEventsToReceive > 0)
            {
                await this.doneProcessingMessages.Task;
            }

            // write back the state
            this.context.Writeback();
            var jstate = JToken.FromObject(this.context.State);

            // continue as new
            innerContext.ContinueAsNew(jstate);

            // The return value is not used.
            return string.Empty;
        }

        public async Task ExecuteBatch()
        {
            if (this.GetFunctionInfo().IsOutOfProc)
            {
                // process all operations in the batch using a single function call
                await this.ExecuteOutOfProcBatch();
            }
            else
            {
                // call the function once per operation in the batch
                foreach (var request in this.operationBatch)
                {
                    await this.ProcessOperationRequestAsync(request);
                }
            }

            // process the lock request, if any
            if (this.lockRequest != null)
            {
                this.ProcessLockRequest(this.lockRequest);
            }
        }

        public void ProcessLockRequest(RequestMessage request)
        {
            this.Config.TraceHelper.EntityLockAcquired(
                this.context.HubName,
                this.context.Name,
                this.context.InstanceId,
                request.ParentInstanceId,
                request.Id.ToString(),
                this.context.IsReplaying);

            System.Diagnostics.Debug.Assert(this.context.State.LockedBy == request.ParentInstanceId, "Lock was set.");

            System.Diagnostics.Debug.Assert(request.LockSet[request.Position].Equals(this.EntityId), "position is correct");
            request.Position++;

            if (request.Position < request.LockSet.Length)
            {
                // send lock request to next entity in the lock set
                var target = new OrchestrationInstance() { InstanceId = EntityId.GetSchedulerIdFromEntityId(request.LockSet[request.Position]) };
                this.Context.SendEntityMessage(target, "op", request);
            }
            else
            {
                // send lock acquisition completed response back to originating orchestration instance
                var target = new OrchestrationInstance() { InstanceId = request.ParentInstanceId };
                var message = new ResponseMessage()
                {
                    Result = "Lock Acquisition Completed", // ignored by receiver but shows up in traces
                };
                this.Context.SendEntityMessage(target, request.Id.ToString(), message);
            }
        }

        private async Task ProcessOperationRequestAsync(RequestMessage request)
        {
            // set context for operation
            this.context.CurrentOperation = request;
            this.context.CurrentOperationResponse = new ResponseMessage();
            this.context.IsNewlyConstructed = !this.context.State.EntityExists;
            this.context.State.EntityExists = true;
            this.context.DestructOnExit = false;

            this.Config.TraceHelper.FunctionStarting(
                this.context.HubName,
                this.context.Name,
                this.context.InstanceId,
                request.Id.ToString(),
                request.Operation,
                this.Config.GetIntputOutputTrace(request.Input),
                FunctionType.Entity,
                this.context.IsReplaying);

            try
            {
                Task invokeTask = this.FunctionInvocationCallback();
                if (invokeTask is Task resultTask)
                {
                    await resultTask;
                }
                else
                {
                    throw new InvalidOperationException("The WebJobs runtime returned a invocation task that is not awaitable!");
                }
            }
            catch (Exception e)
            {
                // exception must be sent with response back to caller
                this.context.CurrentOperationResponse.SetExceptionResult(e, this.context.CurrentOperation.Operation, this.EntityId);

                // the first exception is also handed over to the functions runtime
                if (this.context.OrchestrationException == null)
                {
                    var operationException = new OrchestrationFailureException(
                        $"Operation '{request.Operation}' on entity {this.EntityId} failed: {e.Message}",
                        Utils.SerializeCause(e, MessagePayloadDataConverter.ErrorConverter));
                    this.context.OrchestrationException = ExceptionDispatchInfo.Capture(operationException);
                }

                this.Config.TraceHelper.FunctionFailed(
                    this.context.HubName,
                    this.context.Name,
                    this.context.InstanceId,
                    request.Id.ToString(),
                    request.Operation,
                    reason: e.ToString(),
                    functionType: FunctionType.Entity,
                    isReplay: this.context.IsReplaying);
            }

            // read and clear context
            var response = this.context.CurrentOperationResponse;
            var destructOnExit = this.context.DestructOnExit;
            this.context.CurrentOperation = null;
            this.context.CurrentOperationResponse = null;

            // send response
            // TODO think about how to handle exceptions in signals
            if (!request.IsSignal)
            {
                var target = new OrchestrationInstance() { InstanceId = request.ParentInstanceId };
                var guid = request.Id.ToString();
                var jresponse = JToken.FromObject(response, MessagePayloadDataConverter.DefaultSerializer);
                this.Context.SendEntityMessage(target, guid, jresponse);
            }

            // destruct the entity if the application code requested it
            if (destructOnExit)
            {
                this.context.State.EntityExists = false;
                this.context.State.EntityState = null;
                this.context.CurrentState = null;
                this.context.StateWasAccessed = false;
            }

            this.Config.TraceHelper.FunctionCompleted(
                this.context.HubName,
                this.context.Name,
                this.context.InstanceId,
                request.Id.ToString(),
                request.Operation,
                this.Config.GetIntputOutputTrace(response.Result),
                continuedAsNew: false,
                functionType: FunctionType.Entity,
                isReplay: this.context.IsReplaying);
        }

        private async Task ExecuteOutOfProcBatch()
        {
            object outOfProcResults = null;

            Task invokeTask = this.FunctionInvocationCallback();
            if (invokeTask is Task<object> resultTask)
            {
                outOfProcResults = await resultTask;
            }
            else
            {
                throw new InvalidOperationException("The WebJobs runtime returned a invocation task that does not support return values!");
            }

            var jObj = outOfProcResults as JObject;
            if (jObj == null)
            {
                throw new ArgumentException("Out of proc orchestrators must return a valid JSON schema.");
            }

            var result = jObj.ToObject<OutOfProcResult>();

            // update the state
            this.context.State.EntityExists = result.EntityExists;
            this.context.State.EntityState = result.EntityState;

            // send response messages
            int position = 0;
            foreach (var request in this.OperationBatch)
            {
                if (!request.IsSignal)
                {
                    var response = result.Responses[position++];

                    var target = new OrchestrationInstance()
                    {
                        InstanceId = request.ParentInstanceId,
                    };
                    var responseMessage = new ResponseMessage()
                    {
                        Result = response.Result,
                        ExceptionType = response.IsError ? "Error" : null,
                    };
                    var guid = request.Id.ToString();
                    this.context.SendEntityMessage(target, guid, responseMessage);
                }
            }

            // send signal messages
            foreach (var signal in result.Signals)
            {
                var request = new RequestMessage()
                {
                    ParentInstanceId = this.context.InstanceId,
                    Id = Guid.NewGuid(),
                    IsSignal = true,
                    Operation = signal.Name,
                    Input = signal.Input,
                };
                var target = new OrchestrationInstance()
                {
                    InstanceId = EntityId.GetSchedulerIdFromEntityId(signal.Target),
                };
                this.context.SendEntityMessage(target, "op", request);
            }
        }

        internal class OutOfProcResult
        {
            [JsonProperty("entityExists")]
            public bool EntityExists { get; set; }

            [JsonProperty("entityState")]
            public string EntityState { get; set; }

            [JsonProperty("responses")]
            public List<Response> Responses { get; set; }

            [JsonProperty("signals")]
            public List<Signal> Signals { get; set; }

            public struct Response
            {
                [JsonProperty("result")]
                public string Result { get; set; }

                [JsonProperty("isError")]
                public bool IsError { get; set; }
            }

            public struct Signal
            {
                [JsonProperty("target")]
                public EntityId Target { get; set; }

                [JsonProperty("name")]
                public string Name { get; set; }

                [JsonProperty("input")]
                public string Input { get; set; }
            }
        }
    }
}
