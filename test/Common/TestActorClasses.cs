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
    public class TestActorClasses
    {
        //-------------- an actor representing a chat room -----------------
        // this example shows how to use the C# class API for actors.

        public interface IChatRoom
        {
            DateTime Post(string content);

            List<KeyValuePair<DateTime, string>> Get();
        }

        public class ChatRoom : IChatRoom
        {
            public ChatRoom()
            {
                this.ChatEntries = new SortedDictionary<DateTime, string>();
            }

            public SortedDictionary<DateTime, string> ChatEntries { get; set; }

            // an operation that adds a message to the chat
            public DateTime Post(string content)
            {
                var timestamp = Actor.Context.CurrentUtcDateTime;
                this.ChatEntries.Add(timestamp, content);
                return timestamp;
            }

            // an operation that reads all messages in the chat
            public List<KeyValuePair<DateTime, string>> Get()
            {
                return this.ChatEntries.ToList();
            }
        }

        // boilerplate : must define an actor function for each actor class
        [FunctionName(nameof(ChatRoom))]
        public static void ChatRoomFunction([ActorTrigger] IDurableActorContext context)
        {
            context.Dispatch<ChatRoom>();
        }

        //-------------- an actor testing the proxy generator and serialization for a number of types -----------------

        public interface IProxyTest
        {
            void Void();

            Task VoidAsync();

            int Value(int val);

            Task<int> ValueAsync(int val);

            int? NullableValue(int? val);

            Task<int?> NullableValueAsync(int? val);

            SortedDictionary<DateTime, UserDefinedClass> ComplexType(SortedDictionary<DateTime, UserDefinedClass> val);

            Task<SortedDictionary<DateTime, UserDefinedClass>> ComplexTypeAsync(SortedDictionary<DateTime, UserDefinedClass> val);
        }

        [Serializable]
        public class UserDefinedClass
        {
            public int A { get; set; }
        }

        public class ProxyTest : IProxyTest
        {
            public SortedDictionary<DateTime, UserDefinedClass> ComplexType(SortedDictionary<DateTime, UserDefinedClass> val)
            {
                return val;
            }

            public Task<SortedDictionary<DateTime, UserDefinedClass>> ComplexTypeAsync(SortedDictionary<DateTime, UserDefinedClass> val)
            {
                return Task.FromResult(val);
            }

            public void MultipleArgs(int x, int? y, SortedDictionary<DateTime, UserDefinedClass> z)
            {
                return;
            }

            public int? NullableValue(int? val)
            {
                return val;
            }

            public Task<int?> NullableValueAsync(int? val)
            {
                return Task.FromResult(val);
            }

            public int Value(int val)
            {
                return val;
            }

            public Task<int> ValueAsync(int val)
            {
                return Task.FromResult(val);
            }

            public void Void()
            {
                return;
            }

            public Task VoidAsync()
            {
                return Task.CompletedTask;
            }
        }

        // boilerplate : must define an actor function for each actor class
        [FunctionName(nameof(ProxyTest))]
        public static void ProxyTestFunction([ActorTrigger] IDurableActorContext context)
        {
            context.Dispatch<ProxyTest>();
        }
    }
}
