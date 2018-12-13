# raft-in-rust
[![Build Status](https://travis-ci.org/tmandry/raft-in-rust.svg?branch=master)](https://travis-ci.org/tmandry/raft-in-rust)

A simple implementation of Raft in Rust

This is a toy implementation of Raft. Don't use it for anything real!

## Running

In your working directory, create a file called `servers.txt` with something like the following:

```
127.0.0.1:45440
127.0.0.1:45441
127.0.0.1:45442
```

You can have as many servers as you want. In this example we have three, with server IDs 0, 1, and 2.

To run the server with ID 0:

```
env RUST_LOG=raft=debug,example=debug cargo run 0
```

You can see who the leader is by looking for "Sending heartbeat" messages. Go to that terminal and press enter to apply a change to the state machine. You should see a response in your terminal.

![Raft screenshot](/doc/screenshot.png?raw=true)
