import re
import keyword
from typing import Any, Callable, Dict, List, Tuple, Union

class LispSequencer:
    """
    A Lisp-based control flow sequencer for event-driven workflows.
    
    Supports:
    - @void functions (side effects only)
    - ?boolean functions (returns true/false)
    - !control functions (returns CFL_* control codes)
    
    Function Syntax:
    - No parameters: @fn, ?fn, !fn
    - With parameters: (@fn "arg1" 123), (?fn "arg" 45.6), (!fn "arg1" "arg2" 789)
    - Parameters can be strings or numbers
    - Maximum 10 parameters per function
    
    Primitives:
    - dispatch: Event routing with pattern matching
    - pipeline: Sequential function execution
    - if: Conditional branching
    - cond: Multi-way conditionals
    - debug: Transparent debug message wrapper
    
    Key Features:
    - Stores both original text and tokenized (AST) form during check
    - Execution uses pre-tokenized form for efficiency
    - YAML/JSON compatible: ASTs can be serialized and deserialized
    - Control codes work as both tuples (parser) and lists (JSON/YAML)
    """
    
    CONTROL_CODES = {
        "CFL_CONTINUE", "CFL_HALT", "CFL_TERMINATE", "CFL_RESET", "CFL_DISABLE", "CFL_TERMINATE_SYSTEM","CFL_TERMINATE_FUNCTION",
        "CFL_FUNCTION_RETURN","CFL_FUNCTION_HALT","CFL_FUNCTION_TERMINATE"
    }
    
    MAX_FUNCTION_PARAMS = 10
    
    def __init__(self, handle, run_function: Callable, debug_function: Callable = None):
        """
        Initialize the LispSequencer.
        
        Args:
            handle: Context handle passed to run_function and debug_function
            run_function: Executes functions - signature: 
                         (handle, func_type, func_name, node, event_id, event_data, params=[])
                         - func_type: '@', '?', or '!'
                         - func_name: function name string
                         - params: list of strings and/or numbers
                         Returns:
                         - @ functions: None (side effects only)
                         - ? functions: boolean (True/False)
                         - ! functions: control code string (e.g., "CFL_CONTINUE")
            debug_function: Outputs debug messages - signature: 
                          (handle, message, node, event_id, event_data)
                          If None, debug messages are silently ignored
        """
        self.handle = handle
        self.run_function = run_function
        self.debug_function = debug_function
    
    def check_lisp_instruction(self, lisp_text: str) -> Dict[str, Any]:
        """
        Parse and validate lisp_text sequence.
        Stores both original text and tokenized (AST) form for efficient execution.
        
        Returns:
            Dict with:
                'valid' (bool): Whether the instruction is valid
                'errors' (list): List of validation errors
                'text' (str): Original lisp text
                'ast' (parsed structure): Tokenized/parsed form for execution
                'functions' (list): Functions required by this instruction (e.g., ['@log', '?validate'])
        """
        try:
            # Parse the S-expression
            tokens = self._tokenize(lisp_text)
            ast, _ = self._parse(tokens)
            
            # Validate the AST
            errors = []
            self._validate_expr(ast, errors, context="top-level")
            
            if errors:
                return {"valid": False, "errors": errors, "text": lisp_text, "ast": None, 
                       "functions": []}
            
            # Extract all functions required by this instruction
            functions = self._extract_functions(ast)
            
            # Validate function names are valid Python identifiers
            for func_name in functions:
                func_type = func_name[0]
                name = func_name[1:]
                self._validate_function_name(func_type, name, errors)
            
            if errors:
                return {"valid": False, "errors": errors, "text": lisp_text, "ast": None, 
                       "functions": functions}
            
            return {"valid": True, "errors": [], "text": lisp_text, "ast": ast, 
                   "functions": functions}
            
        except Exception as e:
            return {"valid": False, "errors": [str(e)], "text": lisp_text, "ast": None, 
                   "functions": []}
    
    def run_lisp_instruction(self, node: Any, lisp_instruction: Union[str, List, Dict], 
                            event_id: str, event_data: Any) -> str:
        """
        Execute a lisp instruction using the pre-tokenized form for efficiency.
        
        Args:
            node: Execution context node
            lisp_instruction: Can be:
                - String: lisp text (will be parsed)
                - List: pre-parsed AST (direct execution)
                - Dict: result from check_lisp_instruction (uses 'ast' key)
            event_id: Event identifier
            event_data: Event payload data
            
        Returns:
            Control code string (e.g., "CFL_CONTINUE")
        """
        ast = None
        
        # Handle different input types
        if isinstance(lisp_instruction, dict):
            # Result from check_lisp_instruction - use tokenized form
            if not lisp_instruction.get("valid", False):
                raise ValueError(f"Invalid instruction: {lisp_instruction.get('errors', [])}")
            ast = lisp_instruction["ast"]
        elif isinstance(lisp_instruction, str):
            # Raw text - need to parse
            result = self.check_lisp_instruction(lisp_instruction)
            if not result["valid"]:
                raise ValueError(f"Invalid instruction: {result['errors']}")
            ast = result["ast"]
        else:
            # Assume it's already a parsed AST
            ast = lisp_instruction
        
        # Execute the tokenized AST
        return self._eval(ast, node, event_id, event_data)
    
    def _tokenize(self, text: str) -> List[str]:
        """Convert lisp text into tokens."""
        # Remove comments
        text = re.sub(r';[^\n]*', '', text)
        
        tokens = []
        i = 0
        while i < len(text):
            # Skip whitespace
            if text[i].isspace():
                i += 1
                continue
            
            # Handle string literals
            if text[i] == '"':
                j = i + 1
                while j < len(text) and text[j] != '"':
                    if text[j] == '\\':
                        j += 2  # Skip escaped character
                    else:
                        j += 1
                if j < len(text):
                    tokens.append(text[i:j+1])  # Include quotes
                    i = j + 1
                else:
                    raise SyntaxError("Unterminated string literal")
                continue
            
            # Handle single-character tokens
            if text[i] in '()[]\'':
                tokens.append(text[i])
                i += 1
                continue
            
            # Handle other tokens (symbols, numbers, etc.)
            j = i
            while j < len(text) and not text[j].isspace() and text[j] not in '()[]\'\"':
                j += 1
            tokens.append(text[i:j])
            i = j
        
        return tokens
    
    def _parse(self, tokens: List[str]) -> Tuple[Any, int]:
        """Parse tokens into AST. Returns (ast, tokens_consumed)."""
        if not tokens:
            raise SyntaxError("Unexpected EOF")
        
        token = tokens[0]
        
        # Handle quoted symbols (control codes)
        if token == "'":
            if len(tokens) < 2:
                raise SyntaxError("Quote without following symbol")
            return ("quote", tokens[1]), 2
        
        # Handle lists with parentheses or square brackets
        if token in '([':
            closing = ')' if token == '(' else ']'
            ast = []
            i = 1
            while i < len(tokens) and tokens[i] != closing:
                elem, consumed = self._parse(tokens[i:])
                ast.append(elem)
                i += consumed
            
            if i >= len(tokens):
                raise SyntaxError(f"Unmatched opening {token}")
            
            return ast, i + 1
        
        # Handle closing brackets
        if token in ')]':
            raise SyntaxError(f"Unexpected closing {token}")
        
        # Atoms (strings in quotes, numbers, or bare symbols)
        if token.startswith('"') and token.endswith('"'):
            return token[1:-1], 1  # String literal
        
        # Try to parse as number
        try:
            # Try integer first
            if '.' not in token and 'e' not in token.lower():
                return int(token), 1
            else:
                return float(token), 1
        except ValueError:
            # Not a number, treat as symbol
            return token, 1
    
    def _validate_expr(self, expr: Any, errors: List[str], context: str):
        """Validate expression according to type rules."""
        if not isinstance(expr, list):
            # Atom - check if it's a valid control code in top-level context
            if context == "top-level":
                if isinstance(expr, (tuple, list)) and len(expr) == 2 and expr[0] == "quote":
                    if expr[1] not in self.CONTROL_CODES:
                        errors.append(f"Invalid control code: {expr[1]}")
            return
        
        # Check if this list is actually a quoted control code (for YAML/JSON compatibility)
        if len(expr) == 2 and expr[0] == "quote":
            if context == "top-level":
                if expr[1] not in self.CONTROL_CODES:
                    errors.append(f"Invalid control code: {expr[1]}")
            return
        
        if not expr:
            errors.append("Empty expression")
            return
        
        op = expr[0]
        
        if op == "dispatch":
            self._validate_dispatch(expr, errors)
        elif op == "pipeline":
            self._validate_pipeline(expr, errors)
        elif op == "if":
            self._validate_if(expr, errors)
        elif op == "cond":
            self._validate_cond(expr, errors)
        elif op == "debug":
            self._validate_debug(expr, errors, context)
        elif op in ["and", "or", "not"]:
            self._validate_bool_combinator(expr, errors, context)
        else:
            errors.append(f"Unknown operator: {op}")
    
    def _validate_dispatch(self, expr: List, errors: List[str]):
        """Validate dispatch expression."""
        if len(expr) < 3:
            errors.append("dispatch requires at least event_id and one case")
            return
        
        # Check cases
        has_default = False
        for i in range(2, len(expr)):
            case = expr[i]
            if not isinstance(case, list) or len(case) != 2:
                errors.append(f"Invalid dispatch case: {case}")
                continue
            
            pattern, case_expr = case
            
            # Validate pattern
            if pattern == "default":
                has_default = True
            elif isinstance(pattern, str):
                # Single string pattern is valid
                pass
            elif isinstance(pattern, list):
                # List of strings is valid
                for p in pattern:
                    if not isinstance(p, str):
                        errors.append(f"Dispatch pattern must be string or list of strings: {pattern}")
                        break
            else:
                errors.append(f"Invalid dispatch pattern: {pattern}")
            
            self._validate_expr(case_expr, errors, "top-level")
        
        if not has_default:
            errors.append("dispatch missing default case")
    
    def _validate_pipeline(self, expr: List, errors: List[str]):
        """Validate pipeline expression."""
        if len(expr) < 3:
            errors.append("pipeline requires at least one step and a control code")
            return
        
        # Check steps (all but last)
        for step in expr[1:-1]:
            if isinstance(step, str):
                # Old style: @fn or !fn
                if not (step.startswith('@') or step.startswith('!')):
                    errors.append(f"Pipeline step must be @fn or !fn: {step}")
            elif isinstance(step, list):
                # New style: (@fn "arg1" "arg2") or (!fn "arg1")
                self._validate_function_call(step, errors, allowed_types=['@', '!'])
            else:
                errors.append(f"Pipeline step must be function reference or call: {step}")
        
        # Check final control code (accept both tuple and list for YAML/JSON compatibility)
        final = expr[-1]
        if isinstance(final, (tuple, list)) and len(final) == 2 and final[0] == "quote":
            if final[1] not in self.CONTROL_CODES:
                errors.append(f"Invalid control code in pipeline: {final[1]}")
        else:
            errors.append("Pipeline must end with control code")
    
    def _validate_if(self, expr: List, errors: List[str]):
        """Validate if expression."""
        if len(expr) != 4:
            errors.append(f"if requires predicate, then, and else: got {len(expr)-1} args")
            return
        
        predicate, then_expr, else_expr = expr[1], expr[2], expr[3]
        
        self._validate_predicate(predicate, errors)
        self._validate_expr(then_expr, errors, "top-level")
        self._validate_expr(else_expr, errors, "top-level")
    
    def _validate_cond(self, expr: List, errors: List[str]):
        """Validate cond expression."""
        if len(expr) < 3:
            errors.append("cond requires at least one case and else clause")
            return
        
        has_else = False
        for case in expr[1:]:
            if not isinstance(case, list) or len(case) != 2:
                errors.append(f"Invalid cond case: {case}")
                continue
            
            pred, case_expr = case
            if pred == "else":
                has_else = True
            else:
                self._validate_predicate(pred, errors)
            
            self._validate_expr(case_expr, errors, "top-level")
        
        if not has_else:
            errors.append("cond missing else clause")
    
    def _validate_debug(self, expr: List, errors: List[str], context: str):
        """Validate debug expression."""
        if len(expr) != 3:
            errors.append(f"debug requires message and body: got {len(expr)-1} args")
            return
        
        message, body = expr[1], expr[2]
        
        # Message must be a string
        if not isinstance(message, str):
            errors.append(f"debug message must be a string: {message}")
        
        # Body must be a valid expression that returns control code
        self._validate_expr(body, errors, context)
    
    def _validate_predicate(self, pred: Any, errors: List[str]):
        """Validate that pred is a valid predicate (?fn, and, or, not)."""
        if isinstance(pred, str):
            if not pred.startswith('?'):
                errors.append(f"Predicate must be boolean function (?fn): {pred}")
        elif isinstance(pred, list) and pred:
            if pred[0] in ["and", "or", "not"]:
                # Boolean combinator - OK
                pass
            elif isinstance(pred[0], str) and pred[0].startswith('?'):
                # Function call with parameters: (?fn "arg1" "arg2")
                self._validate_function_call(pred, errors, allowed_types=['?'])
            else:
                errors.append(f"Invalid boolean combinator or predicate: {pred[0]}")
        else:
            errors.append(f"Invalid predicate: {pred}")
    
    def _validate_function_call(self, call: List, errors: List[str], allowed_types: List[str] = None):
        """Validate function call with parameters: (@fn "arg1" 123)."""
        if not isinstance(call, list) or len(call) < 1:
            errors.append(f"Invalid function call: {call}")
            return
        
        func_ref = call[0]
        if not isinstance(func_ref, str) or not func_ref or func_ref[0] not in '@?!':
            errors.append(f"Function call must start with @, ?, or !: {call}")
            return
        
        func_type = func_ref[0]
        func_name = func_ref[1:]
        
        # Check if this type is allowed in this context
        if allowed_types and func_type not in allowed_types:
            expected = '/'.join(allowed_types)
            errors.append(f"Expected {expected} function in this context, got {func_type}{func_name}")
            return
        
        # Validate function name
        self._validate_function_name(func_type, func_name, errors)
        
        # Validate parameters (can be strings or numbers)
        params = call[1:]
        if len(params) > self.MAX_FUNCTION_PARAMS:
            errors.append(f"Function {func_type}{func_name} has too many parameters (max {self.MAX_FUNCTION_PARAMS}): {len(params)}")
        
        for i, param in enumerate(params, 1):
            if not isinstance(param, (str, int, float)):
                errors.append(f"Parameter {i} of {func_type}{func_name} must be a string or number: {param}")
    
    def _validate_bool_combinator(self, expr: List, errors: List[str], context: str):
        """Validate boolean combinator."""
        if context not in ["predicate", "top-level"]:
            errors.append(f"Boolean combinator not allowed in {context}")
            return
        
        op = expr[0]
        if op == "not" and len(expr) != 2:
            errors.append("not requires exactly one argument")
        elif op in ["and", "or"] and len(expr) < 2:
            errors.append(f"{op} requires at least one argument")
    
    def _extract_functions(self, expr: Any) -> List[str]:
        """Extract all function references from AST. Returns full names like '@log'."""
        functions = []
        
        if isinstance(expr, str):
            # Old style: @fn, ?fn, !fn
            if expr and expr[0] in '@?!':
                functions.append(expr)  # Full name: '@log'
        elif isinstance(expr, list):
            # Check if this is a function call: (@fn "arg1" "arg2")
            if (len(expr) > 0 and isinstance(expr[0], str) and 
                expr[0] and expr[0][0] in '@?!'):
                functions.append(expr[0])  # Full name: '@log'
                # Recurse only into parameters (skip the function name at expr[0])
                for item in expr[1:]:
                    functions.extend(self._extract_functions(item))
            else:
                # Not a function call, recurse into all list elements
                for item in expr:
                    functions.extend(self._extract_functions(item))
        
        return functions
    
    def _is_valid_python_identifier(self, name: str) -> bool:
        """Check if name is a valid Python identifier."""
        return name.isidentifier() and not keyword.iskeyword(name)
    
    def _validate_function_name(self, func_type: str, func_name: str, errors: List[str]):
        """Validate that function name is a valid Python identifier."""
        if not func_name:
            errors.append(f"Empty function name for {func_type}")
            return
        
        if not self._is_valid_python_identifier(func_name):
            if func_name[0].isdigit():
                errors.append(f"Invalid function name '{func_type}{func_name}' - cannot start with a digit")
            elif keyword.iskeyword(func_name):
                errors.append(f"Invalid function name '{func_type}{func_name}' - cannot use Python keyword '{func_name}'")
            elif not func_name.replace('_', '').isalnum():
                errors.append(f"Invalid function name '{func_type}{func_name}' - must contain only letters, digits, and underscores")
            else:
                errors.append(f"Invalid function name '{func_type}{func_name}' - must be a valid Python identifier")
    
    def _eval(self, expr: Any, node: Any, event_id: str, event_data: Any) -> str:
        """Evaluate expression and return control code."""
        # Handle control code literals (tuple or list for YAML/JSON compatibility)
        if isinstance(expr, (tuple, list)) and len(expr) == 2 and expr[0] == "quote":
            return expr[1]
        
        # Handle atoms
        if not isinstance(expr, list):
            raise ValueError(f"Invalid expression: {expr}")
        
        if not expr:
            raise ValueError("Empty expression")
        
        op = expr[0]
        
        if op == "dispatch":
            return self._eval_dispatch(expr, node, event_id, event_data)
        elif op == "pipeline":
            return self._eval_pipeline(expr, node, event_id, event_data)
        elif op == "if":
            return self._eval_if(expr, node, event_id, event_data)
        elif op == "cond":
            return self._eval_cond(expr, node, event_id, event_data)
        elif op == "debug":
            return self._eval_debug(expr, node, event_id, event_data)
        else:
            raise ValueError(f"Unknown operator: {op}")
    
    def _eval_dispatch(self, expr: List, node: Any, event_id: str, event_data: Any) -> str:
        """Evaluate dispatch expression."""
        for case in expr[2:]:
            pattern, case_expr = case
            
            # Check if pattern matches
            matched = False
            if pattern == "default":
                matched = True
            elif isinstance(pattern, str):
                matched = (event_id == pattern)
            elif isinstance(pattern, list):
                matched = (event_id in pattern)
            
            if matched:
                return self._eval(case_expr, node, event_id, event_data)
        
        return "CFL_DISABLE"
    
    def _eval_pipeline(self, expr: List, node: Any, event_id: str, event_data: Any) -> str:
        """Evaluate pipeline expression."""
        # Execute all steps
        for step in expr[1:-1]:
            func_type = None
            func_name = None
            params = []
            
            if isinstance(step, str):
                # Old style: @fn or !fn
                func_type = step[0]
                func_name = step[1:]
            elif isinstance(step, list) and len(step) > 0:
                # New style: (@fn "arg1" 123) or (!fn "arg" 45.6)
                func_type = step[0][0]
                func_name = step[0][1:]
                params = step[1:]  # String and number parameters
            
            if func_type:
                result = self.run_function(
                    self.handle, func_type, func_name, node, event_id, event_data, params
                )
                
                # Control functions can halt the pipeline
                if func_type == '!' and result != "CFL_CONTINUE":
                    return result
        
        # Return final control code
        return self._eval(expr[-1], node, event_id, event_data)
    
    def _eval_if(self, expr: List, node: Any, event_id: str, event_data: Any) -> str:
        """Evaluate if expression."""
        predicate, then_expr, else_expr = expr[1], expr[2], expr[3]
        
        if self._eval_predicate(predicate, node, event_id, event_data):
            return self._eval(then_expr, node, event_id, event_data)
        else:
            return self._eval(else_expr, node, event_id, event_data)
    
    def _eval_cond(self, expr: List, node: Any, event_id: str, event_data: Any) -> str:
        """Evaluate cond expression."""
        for case in expr[1:]:
            pred, case_expr = case
            
            if pred == "else" or self._eval_predicate(pred, node, event_id, event_data):
                return self._eval(case_expr, node, event_id, event_data)
        
        return "CFL_HALT"
    
    def _eval_debug(self, expr: List, node: Any, event_id: str, event_data: Any) -> str:
        """Evaluate debug expression - transparent wrapper that emits debug message."""
        message = expr[1]
        body = expr[2]
        
        # Emit debug message if callback is provided
        if self.debug_function:
            self.debug_function(self.handle, message, node, event_id, event_data)
        
        # Execute body and return its result (transparent passthrough)
        return self._eval(body, node, event_id, event_data)
    
    def _eval_predicate(self, pred: Any, node: Any, event_id: str, event_data: Any) -> bool:
        """Evaluate predicate and return boolean."""
        if isinstance(pred, str) and pred.startswith('?'):
            # Old style: ?fn
            func_name = pred[1:]
            result = self.run_function(
                self.handle, '?', func_name, node, event_id, event_data, []
            )
            return bool(result)
        
        elif isinstance(pred, list) and pred:
            op = pred[0]
            
            if op == "and":
                return all(self._eval_predicate(p, node, event_id, event_data) 
                          for p in pred[1:])
            elif op == "or":
                return any(self._eval_predicate(p, node, event_id, event_data) 
                          for p in pred[1:])
            elif op == "not":
                return not self._eval_predicate(pred[1], node, event_id, event_data)
            elif isinstance(op, str) and op.startswith('?'):
                # New style: (?fn "arg1" 123)
                func_name = op[1:]
                params = pred[1:]
                result = self.run_function(
                    self.handle, '?', func_name, node, event_id, event_data, params
                )
                return bool(result)
        
        return False


if __name__ == "__main__":
    # Example usage and testing
    
    print("=" * 60)
    print("LispSequencer - Simplified Version")
    print("=" * 60)
    
    def run_fn(handle, func_type, func_name, node, event_id, event_data, params=[]):
        """Execute a function."""
        params_str = f" with params {params}" if params else ""
        print(f"  → Running {func_type}{func_name}{params_str} on node={node}, event={event_id}")
        
        # Simulate function behavior
        if func_type == '@':
            # Void functions - side effects only, no return
            print(f"    Side effect executed")
            
        elif func_type == '?':
            # Boolean functions - return True/False
            if func_name == "validate_schema":
                return True
            elif func_name == "check_balance":
                return True
            elif func_name == "is_premium":
                return event_data.get("premium", False)
            elif func_name == "check_inventory":
                # Example: use parameter if provided
                minimum = params[0] if params else 0
                print(f"    Checking inventory >= {minimum}")
                return True
            return True
            
        elif func_type == '!':
            # Control functions - return CFL_* codes
            return "CFL_CONTINUE"
    
    def debug_fn(handle, message, node, event_id, event_data):
        """Output debug messages."""
        print(f"  [DEBUG] {message} (node={node}, event={event_id})")
    
    # Create sequencer with simplified interface
    seq = LispSequencer("my-handle", run_fn, debug_fn)
    
    # Example 1: Functions with string and number parameters
    print("\n--- Example 1: Mixed Parameters (Strings and Numbers) ---")
    params_code = """
    (dispatch event_id
      ("order.check"
       (if (?check_inventory 100)
           (pipeline 
             (@log "Order processing" 1)
             (!process_payment "stripe" 99.99)
             (@send_notification "admin@company.com" 200)
             'CFL_CONTINUE)
           (pipeline
             (@log "Insufficient inventory" 0)
             'CFL_HALT)))
      
      (default 'CFL_DISABLE))
    """
    
    result1 = seq.check_lisp_instruction(params_code)
    print(f"\nValidation: {result1['valid']}")
    print(f"Functions: {result1['functions']}")
    
    if result1['valid']:
        print("\nExecuting with event_id='order.check':")
        code = seq.run_lisp_instruction("order-node", result1, "order.check", {})
        print(f"Result: {code}")
    
    # Example 2: Simple dispatch with pipeline
    print("\n--- Example 2: Simple Dispatch ---")
    lisp_code2 = """
    (dispatch event_id
      ("user.created"
       (pipeline @log_start !create_user @log_end 'CFL_CONTINUE))
      
      (["user.updated" "user.modified"]
       (pipeline @log_update !update_user 'CFL_CONTINUE))
      
      (default 'CFL_DISABLE))
    """
    
    result2 = seq.check_lisp_instruction(lisp_code2)
    print(f"\nValidation: {result2['valid']}")
    print(f"Functions: {result2['functions']}")
    
    if result2['valid']:
        print("\nExecuting with event_id='user.created':")
        code = seq.run_lisp_instruction("node1", result2, "user.created", {})
        print(f"Result: {code}")
    
    # Example 3: Complex conditionals with debug
    print("\n--- Example 3: Complex Conditionals with Debug ---")
    lisp_code3 = """
    (dispatch event_id
      ("payment.process"
       (debug "Starting payment processing"
         (cond
           ((and ?validate_schema ?check_balance)
            (debug "Payment validated"
              (pipeline (@log "Valid" 1) !process_payment 'CFL_CONTINUE)))
           
           (?validate_schema
            (debug "Insufficient balance"
              (pipeline (@log "Insufficient" 2) 'CFL_RESET)))
           
           (else
            (debug "Invalid schema"
              (pipeline (@log "Invalid" 3) 'CFL_HALT))))))
      
      (default 'CFL_DISABLE))
    """
    
    result3 = seq.check_lisp_instruction(lisp_code3)
    print(f"\nValidation: {result3['valid']}")
    print(f"Functions: {result3['functions']}")
    
    if result3['valid']:
        print("\nExecuting with event_id='payment.process':")
        code = seq.run_lisp_instruction("payment-node", result3, "payment.process", {})
        print(f"Result: {code}")
    
    # Example 4: Float and negative numbers
    print("\n--- Example 4: Float and Negative Numbers ---")
    float_code = """
    (dispatch event_id
      ("calc.test"
       (pipeline
         (@set_threshold 99.99)
         (@adjust_value -5)
         (@set_rate 0.05)
         (!calculate 100 -20 3.14159)
         'CFL_CONTINUE))
      (default 'CFL_DISABLE))
    """
    
    result4 = seq.check_lisp_instruction(float_code)
    print(f"\nValidation: {result4['valid']}")
    print(f"Functions: {result4['functions']}")
    
    if result4['valid']:
        print("\nExecuting with event_id='calc.test':")
        code = seq.run_lisp_instruction("calc-node", result4, "calc.test", {})
        print(f"Result: {code}")
    
    # Example 5: Invalid - too many parameters
    print("\n--- Example 5: Too Many Parameters (Error) ---")
    too_many = """
    (dispatch event_id
      ("test.event"
       (pipeline 
         (@fn 1 2 3 4 5 6 7 8 9 10 11)
         'CFL_CONTINUE))
      (default 'CFL_DISABLE))
    """
    
    result5 = seq.check_lisp_instruction(too_many)
    print(f"\nValidation: {result5['valid']}")
    if result5['errors']:
        print("Errors:")
        for error in result5['errors']:
            print(f"  - {error}")
    
    # Example 6: Backward compatibility (no parameters)
    print("\n--- Example 6: Backward Compatibility ---")
    compat_code = """
    (dispatch event_id
      ("data.process"
       (pipeline
         @log_start
         (@validate_data "strict")
         !process_data
         @log_end
         'CFL_CONTINUE))
      (default 'CFL_DISABLE))
    """
    
    result6 = seq.check_lisp_instruction(compat_code)
    print(f"\nValidation: {result6['valid']}")
    print(f"Functions: {result6['functions']}")
    
    if result6['valid']:
        print("\nExecuting:")
        code = seq.run_lisp_instruction("data-node", result6, "data.process", {})
        print(f"Result: {code}")
    
    print("\n" + "=" * 60)
    print("Summary:")
    print("  - Simplified initialization: handle, run_function, debug_function")
    print("  - Functions stored as full names: '@log', '?validate', '!process'")
    print("  - Parameters support: strings and numbers (int/float)")
    print("  - Maximum 10 parameters per function")
    print("  - Control functions return CFL_* codes")
    print("  - Boolean functions return True/False")
    print("  - Void functions have side effects only")
    print("  - YAML/JSON compatible: accepts both tuples and lists for control codes")
    print("=" * 60)
    
    # Example 7: YAML/JSON compatibility
    print("\n--- Example 7: YAML/JSON Compatibility ---")
    print("Testing with control code as LIST (from JSON/YAML deserialization)")
    
    # Manually construct AST with list instead of tuple (simulating JSON deserialization)
    manual_ast = ["pipeline", ["@CFL_LOGM", "test_message"], ["quote", "CFL_DISABLE"]]
    
    print(f"Manual AST (list-based): {manual_ast}")
    print("\nExecuting manual AST:")
    code = seq.run_lisp_instruction("yaml-node", manual_ast, "test.event", {})
    print(f"Result: {code}")
    print("\n✓ Successfully executed AST with list-based control code!")
    print("  This means ASTs serialized to JSON/YAML will work correctly.")