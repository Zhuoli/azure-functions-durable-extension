// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Extensions.DurableTask.Tests
{
    internal static class TestActors
    {
        //-------------- a very simple actor that stores a string -----------------
        // it offers two operations:
        // "set" (takes a string, assigns it to the current state, does not return anything)
        // "get" (returns a string containing the current state)

        public static void StringStoreActor([ActorTrigger(ActorClassName = "StringStore")] IDurableActorContext context)
        {
            var state = context.GetState<string>();

            switch (context.OperationName)
            {
                case "set":
                    state.Value = context.GetOperationContent<string>();
                    break;

                case "get":
                    context.Return(state.Value);
                    break;

                default:
                    throw new NotImplementedException("no such operation");
            }
        }

        //-------------- a slightly less trivial version of the same -----------------
        // as before with two differences:
        // - "get" throws an exception if the actor does not already exist, i.e. state was not set to anything
        // - a new operation "delete" deletes the actor, i.e. clears all state

        public static void StringStoreActor2([ActorTrigger(ActorClassName = "StringStore2")] IDurableActorContext context)
        {
            var state = context.GetState<string>();

            switch (context.OperationName)
            {
                case "delete":
                    context.DestructOnExit();
                    break;

                case "set":
                    state.Value = context.GetOperationContent<string>();
                    break;

                case "get":
                    if (context.IsNewlyConstructed)
                    {
                        context.DestructOnExit();
                        throw new InvalidOperationException("must not call get on a non-existing actor");
                    }

                    context.Return(state.Value);
                    break;

                default:
                    throw new NotImplementedException("no such operation");
            }
        }

        //-------------- An actor representing a counter object -----------------

        public static void CounterActor([ActorTrigger(ActorClassName = "Counter")] IDurableActorContext context)
        {
            var state = context.GetState<int>();

            switch (context.OperationName)
            {
                case "increment":
                    state.Value++;
                    break;

                case "add":
                    state.Value += context.GetOperationContent<int>();
                    break;

                case "get":
                    context.Return(state.Value);
                    break;

                case "set":
                    state.Value = context.GetOperationContent<int>();
                    break;

                case "delete":
                    context.DestructOnExit();
                    break;

                default:
                    throw new NotImplementedException("no such actor operation");
            }
        }

        //-------------- An actor representing a phone book, using an untyped json object -----------------

        public static void PhoneBookActor([ActorTrigger(ActorClassName = "PhoneBook")] IDurableActorContext context)
        {
            var state = context.GetState<JObject>();

            switch (context.OperationName)
            {
                case "set":
                    {
                        var (name, number) = context.GetOperationContent<(int, int)>();
                        state.Value[name] = number;
                        break;
                    }

                case "remove":
                    {
                        var name = context.GetOperationContent<string>();
                        state.Value.Remove(name);
                        break;
                    }

                case "lookup":
                    {
                        var name = context.GetOperationContent<string>();
                        context.Return(state.Value[name]);
                        break;
                    }

                case "dump":
                    {
                        context.Return(state.Value);
                        break;
                    }

                case "clear":
                    {
                        context.DestructOnExit();
                        break;
                    }

                default:
                    throw new NotImplementedException("no such actor operation");
            }
        }

        //-------------- An actor representing a phone book, using a typed C# dictionary -----------------

        public static void PhoneBookActor2([ActorTrigger(ActorClassName = "PhoneBook2")] IDurableActorContext context)
        {
            var state = context.GetState<Dictionary<string, decimal>>();

            switch (context.OperationName)
            {
                case "set":
                    {
                        var (name, number) = context.GetOperationContent<(string, decimal)>();
                        state.Value[name] = number;
                        break;
                    }

                case "remove":
                    {
                        var name = context.GetOperationContent<string>();
                        state.Value.Remove(name);
                        break;
                    }

                case "lookup":
                    {
                        var name = context.GetOperationContent<string>();
                        context.Return(state.Value[name]);
                        break;
                    }

                case "dump":
                    {
                        context.Return(state.Value);
                        break;
                    }

                case "clear":
                    {
                        context.DestructOnExit();
                        break;
                    }

                default:
                    throw new NotImplementedException("no such actor operation");
            }
        }

        //-------------- an actor that stores text, and whose state is
        //                  saved/restored to/from storage when the actor is deactivated/activated -----------------
        //
        // it offers three operations:
        // "clear" sets the current value to empty
        // "append" appends the string provided in the content to the current value
        // "get" returns the current value
        // "deactivate" destructs the actor (after saving its current state in the backing storage)

        public static async Task BlobBackedTextStoreActor([ActorTrigger(ActorClassName = "BlobBackedTextStore")] IDurableActorContext context)
        {
            // we define the actor state to be a string builder so we can more efficiently append to it
            var state = context.GetState<StringBuilder>();

            if (context.IsNewlyConstructed)
            {
                // try to load state from existing blob
                var currentFileContent = await context.CallActivityAsync<string>(
                         nameof(TestActivities.LoadStringFromTextBlob),
                         context.Key);
                state.Value = new StringBuilder(currentFileContent ?? "");
            }

            switch (context.OperationName)
            {
                case "clear":
                    state.Value.Clear();
                    break;

                case "append":
                    state.Value.Append(context.GetOperationContent<string>());
                    break;

                case "get":
                    context.Return(state.Value.ToString());
                    break;

                case "deactivate":
                    // first, store the current value in a blob
                    await context.CallActivityAsync(
                        nameof(TestActivities.WriteStringToTextBlob),
                        (context.Key, state.Value.ToString()));

                    // then, destruct this actor (and all of its state)
                    context.DestructOnExit();
                    break;

                default:
                    throw new NotImplementedException("no such operation");
            }
        }
    }
}
