import {
  type DataEvent,
  PortChannel,
  type PortChannelOptions,
} from "@collidor/event";
import type { Type } from "@collidor/command";
import type { Command, COMMAND_RETURN } from "@collidor/command";
import { from, Observable, of, type Subscription } from "rxjs";
import type { ObservableCommandBus } from "../observableCommandBus.ts";
import type { ObservableCommandBusPlugin } from "../observableCommandBus.types.ts";

export type CommandDataEvent = {
  id: string;
  data: any;
};

export type CommandResponseEvent = {
  id: string;
  data: any;
  done: boolean;
  error?: any;
};

export type CommandUnsubscribeEvent = {
  id: string;
};

export type CommandAckEvent = {
  id: string;
};

export type PortChannelPluginMetadata = {
  commandData: CommandDataEvent;
  dataEvent: DataEvent;
};

export type PortChannelPluginOptions = PortChannelOptions & {
  commandTimeout?: number;
  ackTimeout?: number;
};

export class PortChannelPlugin extends PortChannel<any>
  implements ObservableCommandBusPlugin<Command, any> {
  protected commandBus!: ObservableCommandBus<any, any>;
  declare public context: any;

  protected activeIncomingSubscriptions: Map<
    string,
    Subscription | (() => void)
  > = new Map();

  protected commandSubscriptions: Map<
    string,
    (data: any, context: any, dataEvent: any) => void
  > = new Map();

  constructor(options?: PortChannelPluginOptions) {
    super(options);
    if (options?.commandTimeout) {
      this.timeout = options.commandTimeout;
    }
    if (options?.ackTimeout !== undefined) {
      this.ackTimeout = options.ackTimeout;
    } else if (options?.commandTimeout) {
      this.ackTimeout = Math.min(500, options.commandTimeout);
    }
  }

  public unregister(command: Type<Command> | string): void {
    const commandName = typeof command === "string" ? command : command.name;
    const callback = this.commandSubscriptions.get(commandName);
    if (callback) {
      this.unsubscribe(commandName, callback as any);
      this.commandSubscriptions.delete(commandName);
    } else {
      (this.unsubscribe as any)(commandName);
    }
    const unsubscribeName = this.getUnsubscribeName(commandName);
    this.unsubscribe(unsubscribeName);
  }

  protected getCommandInstance(name: string, data: any): Command {
    const constructor = this.commandBus.commandConstructor.get(name);
    if (!constructor) {
      throw new Error(`No class registered for command ${name}`);
    }
    return new constructor(data);
  }

  install(commandBus: ObservableCommandBus<any, any>, context: any): void {
    this.commandBus = commandBus;
    this.context = context;
  }

  register(command: Type<Command>): void {
    const handler = this.commandBus.getHandler(command.name);

    if (!handler && !this.commandBus.providedCommands.has(command.name)) {
      throw new Error(`Command ${command.name} not found locally to expose.`);
    }

    const responseName = this.getResponseName(command.name);
    const unsubscribeName = this.getUnsubscribeName(command.name);

    const unsubscribeHandler = (uData: CommandUnsubscribeEvent) => {
      const activeSub = this.activeIncomingSubscriptions.get(uData.id);
      if (activeSub) {
        if (typeof activeSub === "function") {
          activeSub();
        } else {
          activeSub.unsubscribe();
        }
        this.activeIncomingSubscriptions.delete(uData.id);
      }
    };
    this.subscribe(unsubscribeName, unsubscribeHandler);

    const subscription = (
      commandData: CommandDataEvent,
      _context: any,
      dataEvent: DataEvent,
    ) => {
      // 1. Immediately ACK to the sender
      const ackName = this.getAckName(command.name);
      this.publish(
        ackName,
        { id: commandData.id } as CommandAckEvent,
        { singleConsumer: true, target: dataEvent.source },
      );

      // 2. Instantiate command
      let cmd: Command;
      try {
        cmd = this.getCommandInstance(command.name, commandData.data);
      } catch (err) {
        this.publish(
          responseName,
          { id: commandData.id, data: null, done: true, error: err },
          { singleConsumer: true, target: dataEvent.source },
        );
        return;
      }

      const meta: PortChannelPluginMetadata = { commandData, dataEvent };
      const execHandler = this.commandBus.getHandler(command.name) ?? handler;
      if (!execHandler) {
        this.publish(
          responseName,
          {
            id: commandData.id,
            data: null,
            done: true,
            error: new Error(`No handler registered for ${command.name}`),
          },
          { singleConsumer: true, target: dataEvent.source },
        );
        return;
      }

      // 3. Execute handler
      let rawResult: any;
      try {
        rawResult = execHandler(cmd, this.context, meta);
      } catch (err) {
        this.publish(
          responseName,
          { id: commandData.id, data: null, done: true, error: err },
          { singleConsumer: true, target: dataEvent.source },
        );
        return;
      }

      // 4. Handle Promise / Sync / Observable
      if (rawResult instanceof Promise) {
        let isCancelled = false;
        this.activeIncomingSubscriptions.set(commandData.id, () => {
          isCancelled = true;
        });

        rawResult
          .then((result) => {
            if (isCancelled) return;
            this.publish(
              responseName,
              { id: commandData.id, data: result, done: true },
              { singleConsumer: true, target: dataEvent.source },
            );
            this.activeIncomingSubscriptions.delete(commandData.id);
          })
          .catch((error) => {
            if (isCancelled) return;
            this.publish(
              responseName,
              { id: commandData.id, data: null, done: true, error },
              { singleConsumer: true, target: dataEvent.source },
            );
            this.activeIncomingSubscriptions.delete(commandData.id);
          });
        return;
      }

      if (!(rawResult instanceof Observable)) {
        this.publish(
          responseName,
          { id: commandData.id, data: rawResult, done: true },
          { singleConsumer: true, target: dataEvent.source },
        );
        return;
      }

      // Observable streaming
      const sub = rawResult.subscribe({
        next: (val) => {
          this.publish(
            responseName,
            { id: commandData.id, data: val, done: false },
            { singleConsumer: true, target: dataEvent.source },
          );
        },
        error: (error) => {
          this.publish(
            responseName,
            { id: commandData.id, data: null, done: true, error },
            { singleConsumer: true, target: dataEvent.source },
          );
          this.activeIncomingSubscriptions.delete(commandData.id);
        },
        complete: () => {
          this.publish(
            responseName,
            { id: commandData.id, data: undefined, done: true },
            { singleConsumer: true, target: dataEvent.source },
          );
          this.activeIncomingSubscriptions.delete(commandData.id);
        },
      });

      this.activeIncomingSubscriptions.set(commandData.id, sub);
    };

    this.commandSubscriptions.set(command.name, subscription);
    this.subscribe(command.name, subscription);
  }

  registerStream(command: Type<Command<any, any>>): void {
    if (!this.commandBus.hasLocalStreamHandler(command.name)) {
      throw new Error(`Stream ${command.name} not found`);
    }

    const responseName = this.getResponseName(command.name);
    const unsubscribeName = this.getUnsubscribeName(command.name);

    const subscription = (
      commandData: CommandDataEvent,
      _context: any,
      dataEvent: DataEvent,
    ) => {
      const ackName = this.getAckName(command.name);
      this.publish(ackName, { id: commandData.id } as CommandAckEvent, {
        singleConsumer: true,
        target: dataEvent.source,
      });

      let unsubscribed = false;
      const cmd = this.getCommandInstance(command.name, commandData.data);

      const unsubscribe = this.commandBus.executeLocalStream(
        cmd,
        (data: any, done: boolean, error?: any) => {
          if (unsubscribed) return;
          this.publish(
            responseName,
            { id: commandData.id, data, done, error } as CommandResponseEvent,
            { singleConsumer: true, target: dataEvent.source },
          );
          if (done) {
            unsubscribed = true;
            this.activeIncomingSubscriptions.delete(commandData.id);
          }
        },
        this.context,
      );

      this.activeIncomingSubscriptions.set(commandData.id, () => {
        unsubscribed = true;
        if (typeof unsubscribe === "function") {
          unsubscribe();
        }
      });
    };

    this.subscribe(unsubscribeName, (uData: CommandUnsubscribeEvent) => {
      const active = this.activeIncomingSubscriptions.get(uData.id);
      if (active) {
        if (typeof active === "function") {
          active();
        } else {
          active.unsubscribe();
        }
        this.activeIncomingSubscriptions.delete(uData.id);
      }
    });

    this.commandSubscriptions.set(command.name, subscription);
    this.subscribe(command.name, subscription);
  }

  handler(
    command: Command,
    context: any,
    handler?: (
      command: Command,
      context: any,
    ) => Observable<Command[COMMAND_RETURN]>,
  ): Observable<Command[COMMAND_RETURN]> {
    // 1. Check local handler
    const localHandler = handler ??
      this.commandBus.getHandler(command.constructor.name);
    if (localHandler) {
      try {
        const res = localHandler(command, context ?? this.context);
        if (res instanceof Observable) return res;
        if (res instanceof Promise) return from(res);
        return of(res);
      } catch (err) {
        return new Observable((obs) => obs.error(err));
      }
    }

    // 2. Remote execution via PortChannel Observable
    return new Observable<Command[COMMAND_RETURN]>((subscriber) => {
      const commandName = command.constructor.name;
      const cancel = this.sendStreamWithFailover(
        commandName,
        command.data,
        (data, done, error) => {
          if (error) {
            subscriber.error(error);
          } else {
            if (!done) {
              subscriber.next(data);
            } else {
              if (data !== undefined) {
                subscriber.next(data);
              }
              subscriber.complete();
            }
          }
        },
        {
          timeout: this.timeout,
          ackTimeout: this.ackTimeout,
        },
      );

      return () => {
        cancel();
      };
    });
  }

  streamHandler(
    command: Command,
    context: any,
    next: (data: Command[COMMAND_RETURN], done: boolean, error?: any) => void,
    abortSignal?: AbortSignal,
  ): (() => void) | Promise<() => void> {
    const commandName = command.constructor.name;

    if (this.commandBus.hasLocalStreamHandler(commandName)) {
      return this.commandBus.executeLocalStream(
        command,
        next,
        context ?? this.context,
        abortSignal,
      );
    }

    const cancel = this.sendStreamWithFailover(
      commandName,
      command.data,
      (data, done, error) => {
        next(data, done, error);
      },
      {
        ackTimeout: this.ackTimeout,
      },
    );

    if (abortSignal) {
      abortSignal.addEventListener("abort", () => cancel(), { once: true });
    }

    return cancel;
  }
}
