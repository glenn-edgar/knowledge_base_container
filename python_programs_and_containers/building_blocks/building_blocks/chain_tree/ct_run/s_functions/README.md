# LispSequencer

A Lisp-based control flow sequencer for event-driven workflows with type-safe function markers and compile-time validation.

## Overview

LispSequencer provides a minimal yet powerful S-expression based language for defining event processing workflows. It combines the simplicity of Lisp with type safety enforced at parse time, making it ideal for reliable event-driven systems.

### Key Features

- **Type-safe function markers**: Three function types with compile-time validation
- **Control flow primitives**: dispatch, pipeline, if, cond, debug
- **Pre-compilation**: Parse once, execute many times with tokenized AST
- **CFL_ control codes**: Compatible with existing control flow engines
- **Debug support**: Built-in transparent debug message primitive
- **Parameter support**: Functions accept strings and numbers (up to 10 parameters)
- **YAML/JSON compatible**: ASTs can be serialized and deserialized
- **Simplified API**: Only 2 public methods (check and run)

## Installation

```python
# Copy lisp_sequencer.py to your project
from lisp_sequencer import LispSequencer
```

## Quick Start

```python
from lisp_sequencer import LispSequencer

# Define run_function callback
def run_fn(handle, func_type, func_name, node, event_id, event_data, params=[]):
    if func_type == '@':
        print(f"Side effect: {func_name}")
    elif func_type == '?':
        return True  # Boolean result
    elif func_type == '!':
        return "CFL_CONTINUE"  # Control code

# Create sequencer
seq = LispSequencer("my-app", run_fn)

# Define workflow
workflow = """
(dispatch event_id
  ("user.created"
   (pipeline @log_start !create_user @notify 'CFL_CONTINUE))
  (default 'CFL_DISABLE))
"""

# Compile once
result = seq.check_lisp_instruction(workflow)

# Execute many times
code = seq.run_lisp_instruction("node1", result, "user.created", {})
print(f"Result: {code}")
```

## Language Specification

### Function Markers

Functions are prefixed with special markers that define their type and return behavior:

| Marker | Type | Returns | Usage |
|--------|------|---------|-------|
| `@` | Void | Nothing (side effects only) | `@log_start`, `@send_email` |
| `?` | Boolean | `True` or `False` | `?validate_schema`, `?is_premium` |
| `!` | Control | CFL_* control code | `!process_payment`, `!create_user` |

**Function Syntax:**
```lisp
; No parameters
@log_start
?is_valid
!process_data

; With parameters (strings and numbers)
(@log "Starting process" 1)
(?check_threshold 100 "USD")
(!process_payment "stripe" 99.99)
```

**Function Naming Rules:**
- Must be valid Python identifiers
- Start with letter (a-z, A-Z) or underscore (_)
- Can contain letters, digits, underscores
- Cannot start with digit
- Cannot be Python keywords (e.g., `class`, `if`, `for`)

**Valid Examples:**
```lisp
@log_start  ?is_valid  !process_0  @_helper  ?check2fa
```

**Invalid Examples:**
```lisp
@123invalid  ; Cannot start with digit
!class       ; Python keyword
@my-func     ; Hyphens not allowed
```

### Control Codes

All control codes use the `CFL_` prefix:

| Code | Meaning |
|------|---------|
| `CFL_CONTINUE` | Continue to next step |
| `CFL_HALT` | Stop processing |
| `CFL_TERMINATE` | Terminate workflow |
| `CFL_RESET` | Reset and retry |
| `CFL_DISABLE` | Disable handler |
| `CFL_TERMINATE_SYSTEM` | System-wide termination |

### Parameters

Functions support up to 10 parameters:
- **Strings**: `"hello world"`
- **Integers**: `123`, `-45`
- **Floats**: `99.99`, `-3.14`, `0.05`

```lisp
(@log "Order processed" 12345 99.99)
(?check_balance 1000 "USD")
(!send_notification "user@example.com" "Welcome" 1)
```

## Core Primitives

### 1. dispatch - Event Routing

Routes events to handlers based on pattern matching.

**Syntax:**
```lisp
(dispatch event_id
  (pattern expression)
  (pattern expression)
  ...
  (default expression))
```

**Patterns:**
- Single event: `"event-name"`
- Multiple events: `["event1" "event2" "event3"]`
- Default: `default` (required)

**Example:**
```lisp
(dispatch event_id
  ("user.created"
   (pipeline @log_start !create_user 'CFL_CONTINUE))
  
  (["user.updated" "user.modified"]
   (pipeline @log_update !update_user 'CFL_CONTINUE))
  
  (["payment.success" "payment.completed"]
   (pipeline !process_payment @notify 'CFL_CONTINUE))
  
  (default 'CFL_DISABLE))
```

### 2. pipeline - Sequential Execution

Executes functions in sequence with control flow bubbling.

**Syntax:**
```lisp
(pipeline step1 step2 ... stepN control-code)
```

**Steps:**
- `@void-fn` - Always continues to next step
- `!control-fn` - If returns `CFL_CONTINUE`, continues; otherwise stops and returns that code

**Short-Circuit Behavior:**
```lisp
(pipeline 
  @validate_input      ; Always continues
  !process_data        ; If returns CFL_HALT → stops here
  @send_notification   ; Only runs if !process_data returned CFL_CONTINUE
  'CFL_CONTINUE)       ; Final code if all steps complete
```

**Example:**
```lisp
(pipeline 
  (@log "Starting" 1)
  !validate 
  !process 
  (@notify "admin@company.com")
  'CFL_CONTINUE)
```

### 3. if - Conditional Branch

Binary conditional execution.

**Syntax:**
```lisp
(if predicate then-expression else-expression)
```

**Example:**
```lisp
(if ?is_premium
    (pipeline !process_priority 'CFL_CONTINUE)
    (pipeline !process_standard 'CFL_CONTINUE))
```

**With boolean combinators:**
```lisp
(if (and ?is_authenticated (not ?is_suspended))
    (pipeline @grant_access 'CFL_CONTINUE)
    'CFL_HALT)
```

### 4. cond - Multi-way Conditional

Multi-branch conditional (like switch/case).

**Syntax:**
```lisp
(cond
  (predicate1 expression1)
  (predicate2 expression2)
  ...
  (else expressionN))
```

**Requirements:**
- At least one predicate case
- `else` clause is required
- First match wins (evaluated top to bottom)

**Example:**
```lisp
(cond
  ((and ?validate_schema ?check_balance)
   (pipeline @log_valid !process_payment 'CFL_CONTINUE))
  
  (?validate_schema
   (pipeline @log_insufficient 'CFL_RESET))
  
  (else
   (pipeline @log_invalid 'CFL_HALT)))
```

### 5. debug - Debug Messages

Transparent wrapper for debug output (does not affect control flow).

**Syntax:**
```lisp
(debug "debug message" body-expression)
```

**Behavior:**
- Calls `debug_function(handle, message, node, event_id, event_data)`
- Executes body-expression
- Returns result of body-expression (transparent passthrough)
- If `debug_function` is None, silently ignored

**Example:**
```lisp
(debug "Starting payment validation"
  (if ?validate_payment
      (debug "Payment valid, processing"
        (pipeline !process_payment 'CFL_CONTINUE))
      (debug "Payment invalid"
        'CFL_HALT)))
```

### Boolean Combinators

Combine boolean predicates with logical operators:

**and** - All must be true:
```lisp
(and ?fn1 ?fn2 ?fn3)
```

**or** - Any must be true:
```lisp
(or ?is_admin ?is_moderator)
```

**not** - Negation:
```lisp
(not ?is_suspended)
```

**Nested:**
```lisp
(if (and ?is_authenticated (or ?is_admin ?is_owner) (not ?is_locked))
    'CFL_CONTINUE
    'CFL_HALT)
```

## How to Instantiate

### Constructor

```python
LispSequencer(handle, run_function, debug_function=None)
```

### Parameters

**handle** (Any, required)
- Context identifier passed to all callback functions
- Can be any object (string, dict, class instance, etc.)

**run_function** (Callable, required)
- **Signature**: `run_function(handle, func_type, func_name, node, event_id, event_data, params=[])`
- **Parameters**:
  - `handle`: Context from constructor
  - `func_type`: `'@'`, `'?'`, or `'!'`
  - `func_name`: Function name string (e.g., `'log'`, `'validate'`)
  - `node`: Execution context node
  - `event_id`: Event identifier
  - `event_data`: Event payload
  - `params`: List of strings and/or numbers
- **Returns**:
  - `@` functions: None (side effects only)
  - `?` functions: Boolean (True/False)
  - `!` functions: Control code string (e.g., `"CFL_CONTINUE"`)

**debug_function** (Callable, optional, default=None)
- **Signature**: `debug_function(handle, message, node, event_id, event_data)`
- **Purpose**: Output debug messages from `(debug ...)` expressions
- If None, debug expressions are silently ignored

### Complete Example

```python
from lisp_sequencer import LispSequencer

# Your function registry
function_registry = {}

def run_function(handle, func_type, func_name, node, event_id, event_data, params=[]):
    """Execute a function."""
    key = f"{func_type}{func_name}"
    func = function_registry.get(key)
    
    if not func:
        raise RuntimeError(f"Function {func_type}{func_name} not found")
    
    if func_type == '@':
        # Void function - side effects only
        func(node, event_id, event_data, params)
        return None
    elif func_type == '?':
        # Boolean function - must return True/False
        result = func(node, event_id, event_data, params)
        return bool(result)
    elif func_type == '!':
        # Control function - must return CFL_* code
        return func(node, event_id, event_data, params)

def debug_function(handle, message, node, event_id, event_data):
    """Output debug messages."""
    print(f"[DEBUG] {message} (node={node}, event={event_id})")

# Create sequencer
sequencer = LispSequencer(
    handle="my-application",
    run_function=run_function,
    debug_function=debug_function
)
```

### Minimal Example

```python
sequencer = LispSequencer(
    handle="app",
    run_function=my_run_function
    # No debug_function - debug messages ignored
)
```

## Usage

### Workflow: Check → Execute

```python
# 1. Define workflow (S-expression)
workflow = """
(dispatch event_id
  ("order.created"
   (debug "Processing new order"
     (if ?validate_order
         (pipeline 
           (@log "Valid order" 1)
           !create_order 
           (@notify "admin@company.com")
           'CFL_CONTINUE)
         (pipeline (@log "Invalid order" 0) 'CFL_HALT))))
  (default 'CFL_DISABLE))
"""

# 2. Check and compile (done once)
result = seq.check_lisp_instruction(workflow)

# 3. Validate result
if not result['valid']:
    print(f"Errors: {result['errors']}")
    exit(1)

print(f"Functions required: {result['functions']}")
# Output: ['@log', '?validate_order', '!create_order', '@notify']

# 4. Execute many times (using compiled AST)
for event in events:
    control_code = seq.run_lisp_instruction(
        node="node-1",
        lisp_instruction=result,  # Use pre-parsed result
        event_id=event['id'],
        event_data=event['data']
    )
    print(f"Result: {control_code}")
```

### YAML/JSON Serialization

ASTs can be serialized and deserialized:

```python
import json

# Parse workflow
result = seq.check_lisp_instruction(workflow)

# Serialize to JSON (tuples become lists)
json_str = json.dumps(result)

# Save to file
with open('workflow.json', 'w') as f:
    f.write(json_str)

# Load from file
with open('workflow.json', 'r') as f:
    loaded = json.loads(f.read())

# Execute loaded workflow - WORKS!
code = seq.run_lisp_instruction("node1", loaded, "order.created", {})
```

**Note**: Control codes work as both tuples `("quote", "CFL_CONTINUE")` and lists `["quote", "CFL_CONTINUE"]` for YAML/JSON compatibility.

## API Reference

### LispSequencer Class

#### check_lisp_instruction(lisp_text: str) → dict

Parse and validate instruction, extract functions.

**Returns:**
```python
{
    'valid': bool,              # Is instruction valid?
    'errors': list,             # List of error messages
    'text': str,                # Original source text
    'ast': object,              # Tokenized form for execution
    'functions': list           # Required functions (e.g., ['@log', '?validate'])
}
```

**Example:**
```python
result = seq.check_lisp_instruction("(pipeline @log 'CFL_CONTINUE)")

if result['valid']:
    print(f"Functions: {result['functions']}")
    # Execute workflow
else:
    print(f"Errors: {result['errors']}")
```

#### run_lisp_instruction(node, lisp_instruction, event_id, event_data) → str

Execute instruction and return control code.

**Parameters:**
- `node`: Execution context node
- `lisp_instruction`: Can be:
  - String: lisp text (will be parsed)
  - List: pre-parsed AST (direct execution)
  - Dict: result from `check_lisp_instruction` (uses 'ast' key)
- `event_id`: Event identifier for dispatch
- `event_data`: Event payload (mutable dictionary recommended)

**Returns:** Control code string (e.g., `"CFL_CONTINUE"`)

**Example:**
```python
# Using compiled result (recommended)
result = seq.check_lisp_instruction(workflow)
code = seq.run_lisp_instruction("node1", result, "user.created", user_data)

# Using raw text (will be parsed each time)
code = seq.run_lisp_instruction("node1", "(pipeline @log 'CFL_CONTINUE)", "test", {})

# Using AST directly
ast = ["pipeline", "@log", ["quote", "CFL_CONTINUE"]]
code = seq.run_lisp_instruction("node1", ast, "test", {})
```

## Complete Examples

### Example 1: E-commerce Order Processing

```lisp
(dispatch event_id
  ("order.new"
   (debug "New order received"
     (cond
       ((and ?validate_inventory ?validate_payment)
        (debug "Order validated, processing"
          (pipeline 
            (@reserve_inventory "warehouse1")
            !process_payment 
            (@create_shipment 1)
            (@send_confirmation "customer@email.com")
            'CFL_CONTINUE)))
       
       (?validate_inventory
        (debug "Payment failed, releasing inventory"
          (pipeline (@release_inventory "warehouse1") 'CFL_HALT)))
       
       (else
        (debug "Insufficient inventory"
          'CFL_HALT)))))
  
  ("order.cancelled"
   (debug "Order cancellation requested"
     (pipeline 
       (@release_inventory "warehouse1")
       !refund_payment 
       (@notify_customer "cancelled")
       'CFL_CONTINUE)))
  
  (default 'CFL_DISABLE))
```

### Example 2: User Authentication Flow

```lisp
(dispatch event_id
  (["user.login" "user.reauth"]
   (if (and ?check_credentials (not ?is_locked))
       (if ?requires_2fa
           (debug "2FA required"
             (pipeline (@send_2fa_code "sms") 'CFL_RESET))
           (debug "Login successful"
             (pipeline 
               (@create_session 3600)
               (@log_login "success")
               'CFL_CONTINUE)))
       (debug "Login failed"
         (pipeline 
           (@increment_failed_attempts 1)
           (@log_login "failed")
           'CFL_HALT))))
  
  ("user.verify_2fa"
   (if ?validate_2fa_code
       (debug "2FA verified"
         (pipeline 
           (@create_session 3600)
           (@log_login "2fa_success")
           'CFL_CONTINUE))
       (debug "2FA failed"
         (pipeline (@log_login "2fa_failed") 'CFL_HALT))))
  
  (default 'CFL_DISABLE))
```

### Example 3: State Machine Pattern

```python
# State machine using event_data for state storage
state_machine = """
(dispatch event_id
  ("order.process"
   (cond
     (?is_state_new
      (debug "State: NEW → VALIDATED"
        (if !validate_order
            (pipeline 
              (@set_state "validated")
              (@emit_event "order.validated")
              'CFL_CONTINUE)
            'CFL_HALT)))
     
     (?is_state_validated
      (debug "State: VALIDATED → PAID"
        (if !process_payment
            (pipeline 
              (@set_state "paid")
              (@emit_event "order.paid")
              'CFL_CONTINUE)
            'CFL_HALT)))
     
     (?is_state_paid
      (debug "State: PAID → SHIPPED"
        (pipeline 
          !create_shipment
          (@set_state "shipped")
          (@emit_event "order.shipped")
          'CFL_CONTINUE)))
     
     (?is_state_shipped
      (debug "State: SHIPPED → COMPLETED"
        (if ?is_delivered
            (pipeline 
              (@set_state "completed")
              (@emit_event "order.completed")
              'CFL_CONTINUE)
            'CFL_CONTINUE)))
     
     (else
      (debug "Invalid state transition" 'CFL_HALT))))
  
  ("order.cancel"
   (cond
     ((or ?is_state_new ?is_state_validated)
      (debug "Cancelling order"
        (pipeline 
          (@set_state "cancelled")
          (@emit_event "order.cancelled")
          'CFL_CONTINUE)))
     
     (?is_state_paid
      (debug "Refunding order"
        (pipeline 
          !process_refund
          (@set_state "refunded")
          (@emit_event "order.refunded")
          'CFL_CONTINUE)))
     
     (else
      (debug "Cannot cancel in current state" 'CFL_HALT))))
  
  (default 'CFL_DISABLE))
"""

# Implementation
def run_fn(handle, func_type, func_name, node, event_id, event_data, params):
    if func_type == '?':
        # State checking predicates
        if func_name.startswith("is_state_"):
            state = func_name[9:]  # Remove "is_state_" prefix
            return event_data.get("state") == state
        elif func_name == "is_delivered":
            return event_data.get("delivered", False)
    
    elif func_type == '@':
        # State transitions
        if func_name == "set_state":
            event_data["state"] = params[0]
        elif func_name == "emit_event":
            print(f"Event emitted: {params[0]}")
    
    elif func_type == '!':
        # Business logic
        if func_name == "validate_order":
            return "CFL_CONTINUE" if event_data.get("valid") else "CFL_HALT"
        elif func_name == "process_payment":
            return "CFL_CONTINUE" if event_data.get("paid") else "CFL_HALT"
        # ...

# Execute state machine
seq = LispSequencer("order-system", run_fn, debug_fn)
compiled = seq.check_lisp_instruction(state_machine)

order_data = {"order_id": "12345", "state": "new", "valid": True}

result = seq.run_lisp_instruction("order-node", compiled, "order.process", order_data)
print(f"State: {order_data['state']}, Result: {result}")
```

## Best Practices

### 1. Function Naming

Use descriptive names with underscores:
```lisp
; Good
@log_user_action
?validate_email_format
!process_payment_request

; Avoid (too short)
@log
?valid
!process
```

### 2. Use Debug for Traceability

Wrap complex logic with debug messages:
```lisp
(debug "Entering payment validation"
  (if (and ?validate_schema ?check_balance)
      (debug "Validation passed, processing"
        (pipeline !process 'CFL_CONTINUE))
      (debug "Validation failed"
        'CFL_HALT)))
```

### 3. Reuse Tokenized Forms

Parse once, execute many:
```python
# Parse at startup
workflow = seq.check_lisp_instruction(code)

# Execute in event loop
for event in event_stream:
    code = seq.run_lisp_instruction(node, workflow, event.id, event.data)
```

### 4. Use cond for Multi-way Branches

Prefer `cond` over nested `if` when you have 3+ branches:
```lisp
; Good
(cond
  (?is_admin (pipeline !admin_action 'CFL_CONTINUE))
  (?is_moderator (pipeline !mod_action 'CFL_CONTINUE))
  (?is_user (pipeline !user_action 'CFL_CONTINUE))
  (else 'CFL_HALT))
```

### 5. Store State in event_data

Make `event_data` a mutable dictionary for state machines:
```python
# State stored in event_data
order_data = {"state": "new", "amount": 99.99}
seq.run_lisp_instruction("node1", workflow, "order.process", order_data)
print(order_data["state"])  # Updated by workflow
```

### 6. Use Parameters for Configuration

Pass configuration as parameters:
```lisp
(pipeline 
  (@log "Starting" 1)
  (@set_timeout 300)
  (?check_threshold 1000 "USD")
  (@send_notification "admin@company.com" "Process started")
  'CFL_CONTINUE)
```

## Error Handling

Common validation errors and solutions:

| Error | Cause | Solution |
|-------|-------|----------|
| "Boolean in pipeline" | Used `?fn` in pipeline | Use `@fn` or `!fn` instead |
| "Non-boolean in conditional" | Used `@fn` or `!fn` in if/cond | Use `?fn` instead |
| "dispatch missing default case" | No default clause | Add `(default 'CFL_DISABLE)` |
| "Invalid function name" | Not valid Python identifier | Use valid identifier (letter/underscore start) |
| "cannot start with a digit" | Function name starts with number | Start with letter: `!fn0` not `!0fn` |
| "cannot use Python keyword" | Used reserved word like `class` | Rename: `?is_class` not `?class` |
| "too many parameters" | More than 10 parameters | Reduce to 10 or fewer |
| "Parameter must be string or number" | Invalid parameter type | Use only strings, ints, floats |
| "Unmatched opening parenthesis" | Syntax error | Check parentheses balance |

## Performance Considerations

- **Parse once, execute many**: Store result of `check_lisp_instruction` and reuse
- **Tokenized execution**: No re-parsing on each execution
- **Control flow short-circuit**: Pipeline stops immediately on non-CONTINUE control codes
- **YAML/JSON overhead**: Minimal - control codes work as both tuples and lists

## Type Safety Rules

The parser enforces type constraints at compile time:

| Context | Allowed | Error Example |
|---------|---------|---------------|
| pipeline steps | `@fn`, `!fn` | `?fn` → "Boolean in pipeline" |
| if/cond predicate | `?fn`, `and`, `or`, `not` | `@fn` → "Non-boolean in conditional" |
| Top-level | Must return control code | `?fn` → "Invalid return" |

## Grammar Summary (BNF)

```
<expr> ::= <dispatch> | <pipeline> | <if> | <cond> | <debug> | <control-code>

<dispatch> ::= (dispatch event_id <case>+ (default <expr>))
<case> ::= (<pattern> <expr>)
<pattern> ::= <string> | <string-list>

<pipeline> ::= (pipeline <step>+ <control-code>)
<step> ::= <void-fn> | <control-fn> | <void-fn-call> | <control-fn-call>

<if> ::= (if <predicate> <expr> <expr>)

<cond> ::= (cond <cond-case>+ (else <expr>))
<cond-case> ::= (<predicate> <expr>)

<debug> ::= (debug <string> <expr>)

<predicate> ::= <bool-fn> | <bool-fn-call> | <bool-combinator>
<bool-combinator> ::= (and <predicate>+) | (or <predicate>+) | (not <predicate>)

<void-fn> ::= @<identifier>
<bool-fn> ::= ?<identifier>
<control-fn> ::= !<identifier>

<void-fn-call> ::= (@<identifier> <param>*)
<bool-fn-call> ::= (?<identifier> <param>*)
<control-fn-call> ::= (!<identifier> <param>*)

<param> ::= <string> | <number>
<number> ::= <integer> | <float>

<control-code> ::= 'CFL_CONTINUE | 'CFL_HALT | 'CFL_TERMINATE | 
                   'CFL_RESET | 'CFL_DISABLE | 'CFL_TERMINATE_SYSTEM
```

## License

MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Version History

- **2.0.0** - Simplified API release
  - Removed `get_function`, `store_function`, `validate_functions`
  - Functions stored as full names (`'@log'` not `('@', 'log')`)
  - Added string and number parameter support
  - YAML/JSON compatibility (tuples and lists both work)
  - Simplified to 2 public methods: `check_lisp_instruction`, `run_lisp_instruction`
  
- **1.0.0** - Initial release
  - Core primitives: dispatch, pipeline, if, cond, debug
  - Type-safe function markers
  - Pre-compilation support

## Contributing

Contributions are welcome! Areas for enhancement:
- Additional primitives (loop, try-catch, etc.)
- Performance optimizations
- Extended validation rules
- More comprehensive error messages

## Support

For questions, issues, or feature requests, please open an issue on the project repository.