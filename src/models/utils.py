import yaml

# 加载配置
def load_config(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

config = load_config('config.yaml')