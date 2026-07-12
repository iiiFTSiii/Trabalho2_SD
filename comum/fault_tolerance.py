import json
import os

DEFAULT_STATE_PATH = os.getenv('STATE_FILE', '/app/state.json')


def save_checkpoint(state_data, state_path=None):
    path = state_path or DEFAULT_STATE_PATH
    try:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as handle:
            json.dump(state_data, handle)
        return path
    except Exception as exc:
        print(f"Erro ao salvar checkpoint: {exc}")
        return None


def load_checkpoint(state_path=None):
    path = state_path or DEFAULT_STATE_PATH
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as handle:
                return json.load(handle)
        except Exception as exc:
            print(f"Erro ao carregar checkpoint: {exc}")
    return None
