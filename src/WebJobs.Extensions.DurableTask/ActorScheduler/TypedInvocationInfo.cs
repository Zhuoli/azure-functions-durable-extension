// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Castle.DynamicProxy;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask
{
    /// <summary>
    /// A class encapsulating functionality for capturing, transmitting, and executing typed invocations
    /// on actors using CLR reflection on the runtime object representing the actor.
    /// </summary>
    internal class TypedInvocationInfo : IInterceptor
    {
        private static readonly IFormatter Formatter = new BinaryFormatter();

        public string Name { get; set; }

        public object[] Arguments { get; set; }

        public byte[] SerializedArguments { get; set; }

        // called by Castle proxy when it intercepts the proxy call
        // we use it to record the passed-in invocation information
        void IInterceptor.Intercept(IInvocation invocation)
        {
            if (invocation.GenericArguments != null && invocation.GenericArguments.Length > 0)
            {
                throw new InvalidOperationException("generic arguments are not supported");
            }

            this.Name = invocation.Method.Name;
            this.Arguments = invocation.Arguments;

            if (this.Arguments != null && this.Arguments.Length > 0)
            {
                // for now we use the binary formatter to serialize the arguments
                // because Newtonsoft.Json does not accurately roundtrip CLR runtime types
                // (e.g. converts int32 into int64 which then breaks the reflection
                // because methods do no longer match the argument types)
                using (var stream = new MemoryStream())
                {
                    Formatter.Serialize(stream, this.Arguments);
                    this.SerializedArguments = stream.ToArray();
                }
            }
            else
            {
                this.SerializedArguments = new byte[0];
            }

            // return value is ignored, but if not nullable we have to set it to some value
            // otherwise the proxy library throws an exception
            var methodReturnType = invocation.Method.ReturnType;
            if (methodReturnType != typeof(void) && methodReturnType.IsValueType && Nullable.GetUnderlyingType(methodReturnType) == null)
            {
                invocation.ReturnValue = Activator.CreateInstance(methodReturnType);
            }
        }

        public async Task<object> Invoke(object target)
        {
            IFormatter formatter = new BinaryFormatter();

            if (this.SerializedArguments.Length > 0)
            {
                using (var stream = new MemoryStream(this.SerializedArguments))
                {
                    this.Arguments = (object[])Formatter.Deserialize(stream);
                }
            }

            var targetType = target.GetType();

            object result = targetType.InvokeMember(
               this.Name,
               BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.InvokeMethod,
               null,
               target,
               this.Arguments);

            if (result != null && result is Task task)
            {
                await task;

                if (!task.GetType().IsGenericType)
                {
                    result = null;
                }
                else
                {
                    result = task.GetType().GetProperty("Result").GetValue(task);
                }
            }

            return result;
        }
    }
}
