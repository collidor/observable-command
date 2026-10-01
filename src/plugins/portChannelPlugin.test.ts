import type { MessagePortLike } from "@collidor/event";
import { PortChannelPlugin } from "./portChannelPlugin.ts";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { Command } from "@collidor/command";
import { assertSpyCalls, spy } from "@std/testing/mock";
import { ObservableCommandBus } from "../observableCommandBus.ts";
import { Observable, of, throwError } from "rxjs";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type FakeMessagePort = MessagePortLike & {
  messages: any[];
  sentMessages: any[];
  receivedMessages: any[];
};

function createFakePort(): FakeMessagePort {
  return {
    messages: [],
    sentMessages: [],
    receivedMessages: [],
    postMessage: () => {},
    onmessage: null,
    onmessageerror: null,
  };
}

function connectPorts(...ports: FakeMessagePort[]) {
  for (const port of ports) {
    port.postMessage = function (this: FakeMessagePort, message: any) {
      this.messages.push(message);
      this.sentMessages.push(
        typeof message === "string" ? JSON.parse(message) : message,
      );

      for (const p of ports) {
        if (p === port) continue;
        p.receivedMessages.push(JSON.parse(message));
        p.onmessage?.({ data: message, currentTarget: p } as any);
      }
    };
  }
}

function getNodes<const N extends number>(n: N):
  & Array<{
    port: FakeMessagePort;
    portChannelPlugin: PortChannelPlugin;
    commandBus: ObservableCommandBus<any, PortChannelPlugin>;
  }>
  & { length: N } {
  const ret = [] as any[] as
    & Array<{
      port: FakeMessagePort;
      portChannelPlugin: PortChannelPlugin;
      commandBus: ObservableCommandBus<any, PortChannelPlugin>;
    }>
    & { length: N };

  for (let i = 0; i < n; i++) {
    const port = createFakePort();
    const portChannelPlugin = new PortChannelPlugin();
    const commandBus = new ObservableCommandBus({
      plugin: portChannelPlugin,
    });
    ret.push({
      port,
      portChannelPlugin,
      commandBus,
    });
  }

  connectPorts(...ret.map((r) => r.port));

  for (const r of ret) {
    r.portChannelPlugin.addPort(r.port);
  }

  return ret;
}

class TestCommand extends Command<number, number> {}
class StreamCommand extends Command<number, number> {}

Deno.test("Observable PortChannelPlugin - should execute remote command returning Promise", async () => {
  const [node1, node2] = getNodes(2);

  node1.commandBus.register(TestCommand, async (cmd) => {
    await sleep(10);
    return cmd.data * 2;
  });

  const results: number[] = [];
  await new Promise<void>((resolve, reject) => {
    node2.commandBus.execute(new TestCommand(21)).subscribe({
      next: (val) => results.push(val),
      error: reject,
      complete: resolve,
    });
  });

  assertEquals(results, [42]);
});

Deno.test("Observable PortChannelPlugin - should execute remote command returning synchronous value", async () => {
  const [node1, node2] = getNodes(2);

  node1.commandBus.register(TestCommand, (cmd) => cmd.data + 10);

  const results: number[] = [];
  await new Promise<void>((resolve, reject) => {
    node2.commandBus.execute(new TestCommand(5)).subscribe({
      next: (val) => results.push(val),
      error: reject,
      complete: resolve,
    });
  });

  assertEquals(results, [15]);
});

Deno.test("Observable PortChannelPlugin - should stream remote command returning Observable", async () => {
  const [node1, node2] = getNodes(2);

  node1.commandBus.register(StreamCommand, (cmd) => {
    return of(cmd.data, cmd.data * 2, cmd.data * 3);
  });

  const results: number[] = [];
  await new Promise<void>((resolve, reject) => {
    node2.commandBus.execute(new StreamCommand(10)).subscribe({
      next: (val) => results.push(val),
      error: reject,
      complete: resolve,
    });
  });

  assertEquals(results, [10, 20, 30]);
});

Deno.test("Observable PortChannelPlugin - should tear down remote observable on subscriber unsubscribe", async () => {
  const [node1, node2] = getNodes(2);
  const teardownSpy = spy();

  node1.commandBus.register(StreamCommand, () => {
    return new Observable((subscriber) => {
      const interval = setInterval(() => {
        subscriber.next(1);
      }, 10);

      return () => {
        clearInterval(interval);
        teardownSpy();
      };
    });
  });

  const sub = node2.commandBus.execute(new StreamCommand(0)).subscribe();

  await sleep(50);
  sub.unsubscribe();
  await sleep(20);

  assertEquals(teardownSpy.calls.length, 1);
});

Deno.test("Observable PortChannelPlugin - should propagate remote error through Observable", async () => {
  const [node1, node2] = getNodes(2);

  node1.commandBus.register(TestCommand, () => {
    return throwError(() => "Remote failure");
  });

  const err = await new Promise<any>((resolve, reject) => {
    node2.commandBus.execute(new TestCommand(1)).subscribe({
      next: () => reject(new Error("Should not emit")),
      error: (e) => resolve(e),
      complete: () => reject(new Error("Should not complete")),
    });
  });

  assertEquals(err, "Remote failure");
});

Deno.test("Observable PortChannelPlugin - should fail over to next candidate when first fails to ACK", async () => {
  const [silentNode, activeNode, clientNode] = getNodes(3);

  // activeNode registers handler
  activeNode.commandBus.register(TestCommand, (cmd) => cmd.data * 10);

  // In clientNode, inject silentNode candidate before activeNode
  const silentCandidateId = "silent-dead-node";
  clientNode.portChannelPlugin.idPorts.set(
    silentCandidateId,
    new Map([[silentNode.port, 1]]),
  );
  clientNode.portChannelPlugin.sourceSubscriptions.set(
    TestCommand.name,
    new Set([silentCandidateId, activeNode.portChannelPlugin.id]),
  );

  // Set short ackTimeout for fast failover
  (clientNode.portChannelPlugin as any).ackTimeout = 50;

  const results: number[] = [];
  await new Promise<void>((resolve, reject) => {
    clientNode.commandBus.execute(new TestCommand(7)).subscribe({
      next: (val) => results.push(val),
      error: reject,
      complete: resolve,
    });
  });

  assertEquals(results, [70]);
});

Deno.test("Observable PortChannelPlugin - should timeout when all candidates fail to ACK", async () => {
  const port = createFakePort();
  const portChannelPlugin = new PortChannelPlugin({
    commandTimeout: 100,
    ackTimeout: 50,
    bufferTimeout: 50,
  });
  portChannelPlugin.addPort(port);
  const commandBus = new ObservableCommandBus({
    plugin: portChannelPlugin,
  });

  // Emulate remote peer subscribing
  port.onmessage?.({
    data: JSON.stringify({
      name: TestCommand.name,
      type: "subscribeEvent",
    }),
  } as any);

  const err = await new Promise<any>((resolve, reject) => {
    commandBus.execute(new TestCommand(1)).subscribe({
      next: () => reject(new Error("Should not emit")),
      error: (e) => resolve(e),
      complete: () => reject(new Error("Should not complete")),
    });
  });

  assert(err instanceof Error);
  assert(err.message.includes("Timeout"));
  await sleep(100);
});

Deno.test("Observable PortChannelPlugin - observe() wraps remote callback stream and respects AbortSignal", async () => {
  const [node1, node2] = getNodes(2);
  const teardownSpy = spy();

  node1.commandBus.registerStream(StreamCommand, (_cmd, _ctx, next) => {
    const id = setInterval(() => next(99, false), 10);
    return () => {
      clearInterval(id);
      teardownSpy();
    };
  });

  const ac = new AbortController();
  const values: number[] = [];

  const sub = node2.commandBus.observe(new StreamCommand(0), {}, ac.signal).subscribe({
    next: (v) => values.push(v),
  });

  await sleep(40);
  ac.abort();
  await sleep(20);

  assert(values.length >= 2);
  assertEquals(teardownSpy.calls.length, 1);
  sub.unsubscribe();
});

Deno.test("Observable PortChannelPlugin - bidirectional handshake and availability tracking", async () => {
  const [node1, node2] = getNodes(2);

  assertEquals(node2.commandBus.isAvailable(TestCommand), false);

  node1.commandBus.register(TestCommand, (cmd) => cmd.data);

  assertEquals(node2.commandBus.isAvailable(TestCommand), true);
  assertEquals(node2.commandBus.getAvailableCommands().includes(TestCommand.name), true);

  node1.commandBus.unregister(TestCommand);

  assertEquals(node2.commandBus.isAvailable(TestCommand), false);
});
