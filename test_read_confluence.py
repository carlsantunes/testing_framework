import src.utils.atlassian_utils as qa

atlassian_token = 'ATATT3xFfGF01mB07pLXq9Ss0C-VtN5gKp4Z94ZnFPq6lN_IZ18ZtB1W3KdFinNq6xzRxeAa8BtQnMNtyveBBpGBFRyv6ZaZXUlvu5PrtRHGjjb_bA2s9GE0UWkaJEOVSewKfzJZZR2x4Ux3TCWKjEi0gcMuAOJ2CQN8d43KZ74dZx_iP_Tm6mU=0640C9BA'
user_email = 'cvantunes@ext.worten.pt'
confluence_url = 'https://worten.atlassian.net/wiki/spaces/DARE/pages/6465617921/DW+CL+-+fac_sls_associated_transactions_offline'

confluence = qa.ManageAtlassian(
  atlassian_token,
  user_email)

page_title, page_html_body = confluence.get_page(confluence_url)

nb = confluence.map_design_to_notebook(
  page_title = page_title, 
  page_body = page_html_body,
  confluence_url = confluence_url)