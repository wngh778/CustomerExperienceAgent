import contextvars
from typing import Any

__STATE_CONTEXT_KEY = "state"
__STATE_CONTEXT = contextvars.ContextVar(__STATE_CONTEXT_KEY)

def set_current_state(state : dict):
    global __STATE_CONTEXT
    __STATE_CONTEXT.set(state)
    
def get_current_state() -> dict:
    return __STATE_CONTEXT.get()

def get_current_state_value(key : str) -> Any:
    return __STATE_CONTEXT.get().get(key,None)

def set_current_state_value(key : str , value : Any):
    global __STATE_CONTEXT
    state = __STATE_CONTEXT.get()
    state[key] = value
    __STATE_CONTEXT.set(state)
