import yaml
from core.utils import load_config

config = load_config()

def map_fields(data, field_mapping):
    """将JSON数据映射到各表字段"""
    result = {}
    
    for table, mapping in field_mapping.items():
        table_data = {}
        for model_field, json_field in mapping['fields'].items():
            # 处理嵌套字段
            if isinstance(json_field, str):
                value = data.get(json_field, None)
            elif isinstance(json_field, list):
                value = None
                for field in json_field:
                    if field in data:
                        value = data[field]
                        break
            else:
                value = None
            
            # 空字符串处理为None
            if value == '':
                value = None
                
            table_data[model_field] = value
        
        # 处理复合主键
        key_field = mapping['key']
        if isinstance(key_field, list):
            key_value = tuple(data.get(f, None) for f in key_field)
        else:
            key_value = data.get(key_field, None)
        
        result[table] = {
            'key': key_value,
            'data': table_data
        }
    
    return result