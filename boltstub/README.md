# Bolt Stub Server Script (Stubscript)
This document describes the DSL (domain specific language) that is used to instruct the bolt stub server that is part of testkit.


## General Structure
A script consists of two parts.
The head and the body.
The head contains configuration instructions while the body contains the script that the server follows when a new connection is made.
By default, the server will play through the script with a client exactly once (running in a single thread, so only one connection at a time is possible) and automatically shut down once either the end of the script has been reached (exit with status code 0) or the client sends an unexpected message (exit with non-zero exit code).


## The Head
The head only contains bang lines that start with `!:`.
All but `!: BOLT ${BOLT VERSION}` are optional.

The following options are available:

 * `!: AUTO ${MSG}`  
   Instruct the server to always and automatically respond to any incoming messages of type `${MSG}`.
   The exact response depends on the bolt version the server is configured to use. Most of the time a SUCCESS message will be sent that in case of a HELLO request will contain some predefined metadata to more closely match the response of a real Neo4j server.  
   E.g., `!: AUTO RESET`  
   If the server receives a message that could either be handled by an auto response configured with an auto bang line or by a scripted line that is part of the body, the server will prefer to follow the scripted instructions.  
   It's strongly advised to use this bang line as sparsely as possible as it allows the driver to send the specified message at *any* point in the script.
   Furthermore, with `!: AUTO GOODBYE`, the driver can reach the end of the script at any point which greatly decreases the usefulness of asserting a full script playthrough (i.e. successful Stubserver shutdown). Instead, use auto lines (see further down).
 * `!: BOLT ${BOLT VERSION}` **(required)**  
   Configure the Bolt version the server is speaking.  
   E.g., `!: BOLT 4.0` or `!: BOLT 4`
 * `!: ALLOW RESTART`  
   By default, the server shuts itself down as soon as the end of the script is reached or the client sends an unexpected message.
   If this bang line is part of the head, the server will allow a new connection every time the script has successfully been played through until the end with the previous connection.
   Should the server receive an unexpected message, will it shut down with a non-zero exit code immediately.  
   Note: this still runs the server in a single thread and only one connection at a time is possible.
 * `!: ALLOW CONCURRENT`  
   By default, the server will run in a single thread, only allowing a single connection at a time.
   If this option is present, a threaded version of the server is started that allows any number of concurrent connections.
   Each connection will have their own server state, i.e., where in the script they are.  
   This option implies `!: ALLOW RESTART` as allowing concurrent connections but only allowing the script to be run once makes little sense.
 * `!: HANDSHAKE ${HEX BYTES}`
   By default the server automatically performs a handshake (incl. protocol version negotiation) with the client upon connection (https://7687.org/bolt/bolt-protocol-handshake-specification.html).
   This bang line allows for overwriting the response of the server that normally contains the protocol version which the server wants to speak with the client.  
   E.g. `!: HANDSHAKE FF 00 00 01`
 * `!: HANDSHAKE_DELAY ${DELAY_IN_S}`
    Wait for `${DELAY_IN_S}` (can be int or float) seconds before sending the handshake response.
 * `!: PY ${ARBITRATY PYTHON CODE}`  
   Executes the specified Python code once when the script is loaded regardless of how many times it is played (s. `!: ALLOW RESTART` and `!: ALLOW CONCURRENT` ).
   Only single lines are supported.
   This is particularly useful to initialize variables used later in the script (e.g., in Python lines or Conditional Blocks).


## The Body
The body contains the actual script, i.e., requests and responses that the client is expected to send and receive.
The body consists of client lines (starting with `C:`) that represent requests the server expects to receive from the client, server lines (starting with `S:`) that represent responses the server will send to the client, Python lines (staring with `PY:`) for executing arbitrary Python code, auto lines (starting with `A:`, `?:`, `*:`, or `+:`) that are similar to client lines, but implicitly cause a server response, and control flow lines that allow for loops and similar constructs.
A consecutive sequence of client lines is called **Client Block**, a consecutive sequence of server lines is called **Server Block**, a consecutive sequence of Python lines is called **Python Block**.
Further types of blocks exist for control flow (see further down). A consecutive sequence of blocks is called **Block List**.


### Client and Server Lines
Client and server lines consist of three parts:

 * `C:` or `S:`

   This indicates whether the line is part of a Client or a Server Block.
   For lines that are not the first one in a Client or Server Block (i.e., if the line directly follows a client or server line), this can be omitted to specify a new line of the same type.
   E.g., this is equivalent:

   ```
   C: SOMETHING
   C: ANOTHER_THING
   ```

   ```
   C: SOMETHING
      ANOTHER_THING
   ```

 * A message name as described at https://7687.org/.
   E.g., `RUN`.  
   The names available depends on the protocol version the stub server is running
   (see `!: BOLT ${BOLT VERSION}`)

 * A whitespace separated list of data fields in JOLT format.
   E.g., `"RETURN 1 as n" {} {}`.
   Cf. [[DRV16] Jolt](https://docs.google.com/document/u/0/d/1QK4OcC0tZ08lKqVr-3-z8HpPY9jFeEh6zZPhMm15D-w/edit) (internal document).
   Note, that Stubscript took the liberty to support a simple representation of dictionaries (whenever unambiguous) for improved backwards compatibility, brevity, and readability of Stubscripts.
   * By default, the Stubserver derives the appropriate PackStream version to use from the specified Bolt version.
     However, you can manually overwrite this by appending a version to the JOLT object keys.
     Given some JOLT key `"T"`, you can append a `"vX"` giving `"TvX"`, where `X` is the version of PackStream you wish to overwrite.
     E.g., assume the specified Bolt version dictates PackStream version 123, then `{"Z": "1"}` would be encoded as integer in the way PackStream version 123 specifies.
     `{"Zv10": "1"}` would alter the encoding of that one element to follow PackStream version 10's specification.

Example:
```
C: BEGIN {"mode": "r", "db": "adb"}
S: SUCCESS {}
C: RUN "RETURN 1 as n" {} {}
C: PULL {"n": 1000}
S: SUCCESS {"fields": ["n"]}
RECORD [1]
SUCCESS {"type": "r"}
C: COMMIT
S: SUCCESS {}
```


### Client Lines
There is special syntax only supported by the data fields of client lines.
It is split by the json construct it concerns:
 * JSON Strings that are no object keys:
   * The string will be **unescaped** before it is matched:
     `\\` and `\*` are turned into `\` and `*` respectively.
   * The special string `"*"` (before unescaping) matches **everything**. Regardless of type or data.  
     E.g., `C: RUN "*"` would match `C: RUN "RETURN 1 as n"` but also `C: RUN 1.2`, not however `C: RUN 1 2`.
   * The wildcard also works for JOLT encoded objects.  
     E.g., `C: RUN {"Z": "*"}` will match any integer: `C: RUN 1` and `C: RUN 2`, but not `C: RUN 1.2` or `C: RUN "*"`.
 * JSON object keys:
   * The key will be **unescaped** before it is matched:
     `\\`, `\[`, `\]`, `\{`, and `\}` are turned into `\`, `[`, `]`, `{`, and `}` respectively.
   * If the escaped key starts with `[` and ends with `]`, the corresponding key/value pair is **optional**.  
     E.g., `C: PULL {"[n]": 1000}` matches `C: PULL {}` and `C: PULL {"n": 1000}`, but not `C: PULL {"n": 1000, "m": 1001}`,  `C: PULL {"n": 1}`, or  `C: PULL null`.
   * If the escaped key ends on `{}` after potential optional-brackets (s. above) have been stripped, the corresponding value will be compared **sorted** if it's a list.  
     E.g., `C: MSG {"foo{}": [1, 2]}` will match `C: MSG {"foo": [1, 2]}` and `C: MSG {"foo": [2, 1]}`, but `C: MSG {"foo{}": "ba"}` will not match `C: MSG {"foo": "ab"}`.
   * Example for **optional** and **sorted**: `C: MSG {"[foo{}]": [1, 2]}`.


### Server Lines
There are server lines that contain instructions to the bolt server rather than messages to be sent to the client.
 * `S: <EXIT>` will make the stub server close all connections and exit.
 * `S: <NOOP>` will make the server send a noop packages (two null-bytes)
 * `S: <RAW> ${HEX BYTES}` will make the server send the raw bytes over the wire. No chunking or other processing is applied.  
   E.g., `S: <RAW> 00 00` is equivalent to `S: <NOOP>`.
 * `S: <SLEEP> ${SECONDS}` will make the server wait for the specified amount of time.  
   E.g. `S: <SLEEP> 0.5`.


### Python Lines
Executes arbitrary Python code.
This is thread safe (for usage with concurrent connections), and the variable space is shared across all connections.
The thread safety is simply achieved by running all Python lines mutually exclusively.
This means that it's easy to cause deadlocks when using synchronization primitives in Python lines.
Multi-line expressions are currently not supported!

Example:
```
PY: foo = 1 + 2
```


### Flow Control
#### Alternative Blocks
Allow the script to take one of many possible paths (**Block Lists**)
If ambiguous, the first Block List that matches will be preferred.

```
{{
    C: THIS_REQUEST
    S: THIS_RESPONSE
----
    C: THAT_REQUEST
    S: THAT_RESPONSE
}}
```

*Note* that the first block of each Block List must not be a Server Block, neither can it be another block that potentially starts with a Server Block.
The same applies to Python Blocks.


#### Parallel Blocks
Allow the script to take multiple paths (**Block Lists**) at the same time.
The order between the inner blocks doesn't matter and can be interwoven, but the order within each block will be respected.
If ambiguous, the first Block List that matches will be preferred.

```
{{
    C: THIS
    C: THAT
++++
    C: OTHER
}}
```

This script will accept any of the following series of client requests:

```
C: THIS, C: THAT, C: OTHER
```

```
C: THIS, C: OTHER, C: THAT
```

```
C: OTHER, C: THIS, C: THAT
```

*Note* that the first block of each Block List must not be a Server Block, neither can it be another block that potentially starts with a Server Block.
The same applies to Python Blocks.


#### Optional Block
Allows the script to skip over a **Block List** if needed. If ambiguous, not skipping will be preferred.

```
{?
    C: MAY_OR_MAY_NOT
    S: IF_THEN_THIS_REPONSE
?}
```

Note that the first block of the inner Block List must not be a Server Block, neither can it be another block that potentially starts with a Server Block.
Note that the first block of each block that potentially follows the Optional Block must not be a ServerBlock, neither can it be another block that potentially starts with a Server Block.
The same applies to Python Blocks.


#### Repeat 0 Block
Allow the script to play a *Block List* 0 to any number of times.

```
{*
    C: REQUEST
    S: RESPONSE
*}
```

*Note* that the first block of the inner Block List as well as the first block of the following block must not be a Server Block, neither can it be another block that potentially starts with a Server Block.
The same applies to Python Blocks.


#### Repeat 1 Block
Allow the script to play a Block List 1 to any number of times.

```
{+
    C: REQUEST
    S: RESPONSE
+}
```

*Note* that the first block of the inner Block List as well as the first block of the following block must not be a Server Block, neither can it be another block that potentially starts with a Server Block.
The same applies to Python Blocks.


#### Simple Block
Explicitly group blocks together.

```
{{
    C: REQUEST
    S: RESPONSE
}}
```

This is mostly useful in conjunction with Conditional Blocks (s. below).


#### Conditional Blocks
Allow the script to take one of two possible paths (**Block Lists**) depending on a condition that can be any arbitrary Python expression.

```
IF: some_condition == 123
    S: SOME_RESPONSE
ELIF: some < other < condition
    S: SOME_OTHER_RESPONSE
ELSE:
    S: ELSE_THIS_RESPONSE
       AND_THAT_RESPONSE
S: ALWAYS_THIS_RESPONSE
```

As you might expect, you can have any number of `ELIF`s (including 0) and the `ELSE` is optional.

Note in the above example, that the two server lines in the `ELSE` branch are one block because they use the implicit block syntax.
Generally, Stubscript is whitespace insensitive and that also applies here.
To avoid confusion or to group multiple blocks together, use Simple Blocks (s. above) or other grouping blocks.

Example:
```
IF: some_condition == 123
{{
    S: SOME_RESPONSE
}}
ELIF: some < other < condition
{{
    S: SOME_OTHER_RESPONSE
}}
ELSE:
{{
    S: ELSE_THIS_RESPONSE
       AND_THAT_RESPONSE
    C: THEN_THIS_REQUEST
}}
S: ALWAYS_THIS_RESPONSE
```


#### Auto Lines
Most of the time, auto lines like `A: ${MSG}` are the better alternative to the bang line `!: AUTO ${MSG}` as they allow you to specify when and how many times to automatically reply to a message.

Auto lines are very similar to client lines, but they
 * automatically trigger a server response that depends on the message received.
   The response is the same as the auto bang line would trigger.
 * don't have a shorthand notation in blocks, to avoid ambiguity.  
   **Wrong**
   ```
   A: SOMETHING
      ANOTHER_THING
   ```

   **Correct**
   ```
   A: SOMETHING
   A: ANOTHER_THING
   ```


Auto lines define a shorthand notation for loops to enable a more compact way of allowing different numbers of repetitions of the message.

```
?: ${MSG}
```

```
*: ${MSG}
```

```
+: ${MSG}
```

are internally expanded to

```
{?
    A: ${MSG}
?}
```

```
{*
    A: ${MSG}
*}
```

```
{+
    A: ${MSG}
+}
```


### Ambiguity
Whenever there is ambiguity in the path of the script, the first option (top to bottom) will be chosen.
Furthermore, each block that might or might not be executed due to ambiguity must not start with a Server Block, neither can it start with another block that potentially starts with a Server Block.

| Type of Block     | Inner Block List(s) can start with a Server/Python Block | Can be followed by a Server/Python Block |
|-------------------|----------------------------------------------------------|------------------------------------------|
| Block List        | ✅                                                        | Depends on the last inner block          |
| Client Block      | —                                                        | ✅                                        |
| Server Block      | —                                                        | ✅                                        |
| Python Block      | —                                                        | ✅                                        |
| Alternative Block | ❌                                                        | ✅                                        |
| Optional Block    | ❌                                                        | ❌                                        |
| Repeat 0 Block    | ❌                                                        | ❌                                        |
| Repeat 1 Block    | ❌                                                        | ❌                                        |
| Simple Block      | ✅                                                        | Depends on the last inner block          |
| Conditional Block | ✅                                                        | If last inner block of each branch can   |


## Whitespace
Stubscript is insensitive to whitespace except for a few places:
 * Bang Lines with multiple words require some whitespace to separate the words.
 * Bang Lines with arguments (e.g., `!: AUTO ${MSG}`) requires some whitespace before the argument.
 * Client and Server Lines require some whitespace between message name and data.
 * Instructional Server Lines with arguments (e.g., `S: <RAW> ${HEX BYTES}`) requires some whitespace before the argument.
 * The data part of Client and Server lines needs to be a whitespace separated list of JSON/JOLT encoded fields.
 * At least one newline character must separate all client lines, server lines, Python lines, auto lines, and control flow structures (i.e., `{{`, `}}`, `----`, `++++`, `{*`, `*}`, `{+`, `+}`, `IF: ...`, `ELIF: ...`, and `ELSE:`).

*Note*: Hexadecimal arguments (e.g., `!: HANDSHAKE ${HEX BYTES}` or
`S: <RAW> ${HEX BYTES}`) do not require whitespace between octets.
Adding whitespace between nibbles will make them be interpreted as shorthand byte notation.

Example: All these hexadecimal notations are equivalent:
```
00 05 12 0F
```

```
0005120F
```

```
0 5 12    F
```

```
0 0512F
```

Remember: just because you can, doesn't mean you should ;)


## Comments
Stubscript allows you to put comments anywhere in the head and the body, as long as they are on their own lines.
Comments begin with `#`.  
Example:
```
# This is a comment.
    # This is, too!
#This as well, but a space after `#` looks nicer, doesn't it?
```

**WRONG** example (this isn't a valid comment and will cause an error):
```
C: REQUEST  # this will be tried to be parsed as request data fields
```
