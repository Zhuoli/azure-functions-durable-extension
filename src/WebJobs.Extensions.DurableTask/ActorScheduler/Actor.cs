// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;

namespace Microsoft.Azure.WebJobs
{
    /// <summary>
    /// Statically accessible context for actor operations.
    /// </summary>
    public static class Actor
    {
        private static readonly AsyncLocal<IDurableActorContext> ActorContext
            = new AsyncLocal<IDurableActorContext>();

        /// <summary>
        /// The context of the currently executing actor.
        /// </summary>
        public static IDeterministicExecutionContext Context => ActorContext.Value;

        /// <summary>
        /// The key of the currently executing actor.
        /// </summary>
        public static string Key => ActorContext.Value.Key;

        /// <summary>
        /// The actor reference for the currently executing actor.
        /// </summary>
        public static ActorId Self => ActorContext.Value.Self;

        internal static void SetContext(IDurableActorContext context)
        {
            ActorContext.Value = context;
        }
    }
}
