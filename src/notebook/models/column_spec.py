## Column class (stores column metadata)
class ColumnSpec:
  def __init__(self, name, data_type, description, primary_key = 'N/A', validation=[{'rule_description': 'N/A', 'error_message': 'N/A'}], flg_to_encrypt = False):
    self.name = name
    self.data_type = data_type
    self.description = description
    if flg_to_encrypt == True:
      if self.name in ('dtm_created_at', 'dtm_updated_at') or "suk_" in self.name:
        self.flg_is_encrypted = False
      elif flg_to_encrypt == True:
        self.flg_is_encrypted = True
    else:
      self.flg_is_encrypted = False
    self.validation = validation
    self.primary_key = primary_key
  	
  def __str__(self):
    return f"Column name: {self.name}, data_type: {self.data_type}, description: '{self.description}', primary_key: {self.primary_key}, flg_is_encrypted: {self.flg_is_encrypted}, validation: {self.validation}"
