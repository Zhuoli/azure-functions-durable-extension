// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Castle.DynamicProxy;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace Microsoft.Azure.WebJobs
{
    /// <summary>
    /// Extends the client and actor contexts to support typed object-style invocations using CLR interfaces.
    /// </summary>
    public static class TypedInvocationExtensions
    {
        private static ProxyGenerator proxyGenerator = new ProxyGenerator();

        /// <summary>
        /// Signals an actor to perform an operation.
        /// </summary>
        /// <typeparam name="T">The actor interface for this operation.</typeparam>
        /// <param name="client">The client.</param>
        /// <param name="actorId">The target actor.</param>
        /// <param name="invocation">The actor invocation.</param>
        /// <param name="taskHubName">The TaskHubName of the target actor.</param>
        /// <param name="connectionName">The name of the connection string associated with <paramref name="taskHubName"/>.</param>
        /// <returns>A task that completes when the message has been reliably enqueued.</returns>
        /// <remarks>
        /// All method arguments are serialized with <see cref="BinaryFormatter"/>. User-defined
        /// types must thus be marked as serializable.
        /// </remarks>
        public static Task SignalActor<T>(this IDurableOrchestrationClient client, ActorId actorId, Action<T> invocation, string taskHubName = null, string connectionName = null)
            where T : class
        {
            var invocationInfo = new TypedInvocationInfo();
            var proxy = proxyGenerator.CreateInterfaceProxyWithoutTarget<T>(invocationInfo);
            invocation(proxy);
            return client.SignalActor(actorId, invocationInfo.Name, invocationInfo.SerializedArguments, taskHubName, connectionName);
        }

        /// <summary>
        /// Sends a signal to an actor to perform an operation. Does not wait for a response, result, or exception.
        /// </summary>
        /// <typeparam name="T">The actor interface for this operation.</typeparam>
        /// <param name="context">The context.</param>
        /// <param name="actor">The target actor.</param>
        /// <param name="invocation">The actor invocation.</param>
        /// <remarks>
        /// All method arguments are serialized with <see cref="BinaryFormatter"/>. User-defined
        /// types must thus be marked as serializable.
        /// </remarks>
        public static void SignalActor<T>(this IDeterministicExecutionContext context, ActorId actor, Action<T> invocation)
            where T : class
        {
            var invocationInfo = new TypedInvocationInfo();
            var proxy = proxyGenerator.CreateInterfaceProxyWithoutTarget<T>(invocationInfo);
            invocation(proxy);
            context.SignalActor(actor, invocationInfo.Name, invocationInfo.SerializedArguments);
        }

        /// <summary>
        /// Calls an operation on an actor, passing an argument, and returns the result asynchronously.
        /// </summary>
        /// <typeparam name="T">The actor interface for this operation.</typeparam>
        /// <param name="context">The context.</param>
        /// <param name="actorId">The target actor.</param>
        /// <param name="invocation">The invocation.</param>
        /// <returns>A task representing the completion of the operation on the actor.</returns>
        /// <exception cref="LockingRulesViolationException">if the context already holds some locks, but not the one for <paramref name="actorId"/>.</exception>
        /// <remarks>
        /// All method arguments are serialized with <see cref="BinaryFormatter"/>. User-defined
        /// types must thus be marked as serializable.
        /// </remarks>
        public static Task CallActorAsync<T>(this IInterleavingContext context, ActorId actorId, Action<T> invocation)
            where T : class
        {
            var invocationInfo = new TypedInvocationInfo();
            var proxy = proxyGenerator.CreateInterfaceProxyWithoutTarget<T>(invocationInfo);
            invocation(proxy);
            return context.CallActorAsync(actorId, invocationInfo.Name, invocationInfo.SerializedArguments);
        }

        /// <summary>
        /// Calls an operation on an actor, passing an argument, and returns the result asynchronously.
        /// </summary>
        /// <typeparam name="TActorInterface">The actor interface for this operation.</typeparam>
        /// <typeparam name="TResult">The JSON-serializable result type of the operation.</typeparam>
        /// <param name="context">The context.</param>
        /// <param name="actorId">The target actor.</param>
        /// <param name="invocation">The invocation.</param>
        /// <returns>A task representing the result of the operation.</returns>
        /// <exception cref="LockingRulesViolationException">if the context already holds some locks, but not the one for <paramref name="actorId"/>.</exception>
        /// <remarks>
        /// All method arguments are serialized with <see cref="BinaryFormatter"/>. User-defined
        /// types must thus be marked as serializable.
        /// </remarks>
        public static Task<TResult> CallActorAsync<TActorInterface, TResult>(this IInterleavingContext context, ActorId actorId, Func<TActorInterface, TResult> invocation)
             where TActorInterface : class
        {
            var invocationInfo = new TypedInvocationInfo();
            var proxy = proxyGenerator.CreateInterfaceProxyWithoutTarget<TActorInterface>(invocationInfo);
            invocation(proxy);
            return context.CallActorAsync<TResult>(actorId, invocationInfo.Name, invocationInfo.SerializedArguments);
        }

        /// <summary>
        /// Calls an operation on an actor, passing an argument, and returns the result asynchronously.
        /// </summary>
        /// <typeparam name="TActorInterface">The actor interface for this operation.</typeparam>
        /// <typeparam name="TResult">The JSON-serializable result type of the operation.</typeparam>
        /// <param name="context">The context.</param>
        /// <param name="actorId">The target actor.</param>
        /// <param name="invocation">The invocation.</param>
        /// <returns>A task representing the result of the operation.</returns>
        /// <exception cref="LockingRulesViolationException">if the context already holds some locks, but not the one for <paramref name="actorId"/>.</exception>
        /// <remarks>
        /// All method arguments are serialized with <see cref="BinaryFormatter"/>. User-defined
        /// types must thus be marked as serializable.
        /// </remarks>
        public static Task<TResult> CallActorAsync<TActorInterface, TResult>(this IInterleavingContext context, ActorId actorId, Func<TActorInterface, Task<TResult>> invocation)
             where TActorInterface : class
        {
            var invocationInfo = new TypedInvocationInfo();
            var proxy = proxyGenerator.CreateInterfaceProxyWithoutTarget<TActorInterface>(invocationInfo);
            invocation(proxy);
            return context.CallActorAsync<TResult>(actorId, invocationInfo.Name, invocationInfo.SerializedArguments);
        }

        /// <summary>
        /// Dynamically dispatches the incoming actor operation using reflection.
        /// </summary>
        /// <typeparam name="T">The class to use for actor instances.</typeparam>
        /// <returns>A task that completes when the dispatched operation has finished.</returns>
        /// <remarks>
        /// If the actor's state is null, an object of type <typeparamref name="T"/> is created first. Then, reflection
        /// is used to try to find a matching method. This match is based on the method name
        /// (which is the operation name) and the argument list (which is the operation content, deserialized into
        /// an object array).
        /// </remarks>
        public static async Task Dispatch<T>(this IDurableActorContext context)
            where T : new()
        {
            var invocationInfo = new TypedInvocationInfo()
            {
                Name = context.OperationName,
                SerializedArguments = context.GetOperationContent<byte[]>(),
            };

            var state = context.GetState<T>();
            if (state.Value == null)
            {
                state.Value = new T();
            }

            var result = await invocationInfo.Invoke(state.Value);

            context.Return(result);
        }
    }
}
