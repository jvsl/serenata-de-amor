from urllib.request import urlopen
import json
import numpy as np
import pandas as pd

data = pd.read_csv('data/2016-08-08-last-year.xz',
                   parse_dates=[16],
                   dtype={'document_id': np.str,
                          'congressperson_id': np.str,
                          'congressperson_document': np.str,
                          'term_id': np.str,
                          'cnpj_cpf': np.str,
                          'reimbursement_number': np.str})

is_cnpj = data['cnpj_cpf'].str.len() == 14.
cnpj_list = set(data.loc[is_cnpj, 'cnpj_cpf'])

columns = ['atividade_principal', 'data_situacao', 'tipo', 'nome', 'telefone', 'atividades_secundarias', 'situacao', 'bairro', 'logradouro', 'numero', 'cep', 'municipio', 'uf', 'abertura', 'natureza_juridica', 'fantasia', 'cnpj', 'ultima_atualizacao', 'status', 'complemento', 'email', 'efr', 'motivo_situacao', 'situacao_especial', 'data_situacao_especial']

dataset_path = 'data/cnpj_info.xz'
dataset = pd.read_csv(dataset_path)

already_fetched = set(dataset['cnpj'].str.replace(r'[./-]', ''))
remaining_cnpjs = list(cnpj_list - already_fetched)
for cnpj in remaining_cnpjs:
    print('Fetching CNPJ %s' % cnpj)
    json_contents = urlopen('http://receitaws.com.br/v1/cnpj/%s' % cnpj).read().decode('utf-8')
    attributes = json.loads(json_contents)
    dataset = dataset.append(attributes, ignore_index=True)
    dataset.to_csv(dataset_path,
                   compression='xz',
                   encoding='utf-8',
                   index=False)
