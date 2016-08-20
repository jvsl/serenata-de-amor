import json
import numpy as np
import os.path
import pandas as pd
import signal
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen

INFO_DATASET_PATH = 'data/cnpj_info.xz'

class TimeoutException(Exception):
    pass

def load_info_dataset():
    if os.path.exists(INFO_DATASET_PATH):
        return pd.read_csv(INFO_DATASET_PATH)
    else:
        return pd.DataFrame(columns=['atividade_principal',
                                     'data_situacao',
                                     'tipo',
                                     'nome',
                                     'telefone',
                                     'atividades_secundarias',
                                     'situacao',
                                     'bairro',
                                     'logradouro',
                                     'numero',
                                     'cep',
                                     'municipio',
                                     'uf',
                                     'abertura',
                                     'natureza_juridica',
                                     'fantasia',
                                     'cnpj',
                                     'ultima_atualizacao',
                                     'status',
                                     'complemento',
                                     'email',
                                     'efr',
                                     'motivo_situacao',
                                     'situacao_especial',
                                     'data_situacao_especial'])

def remaining_cnpjs(info_dataset):
    dataset = pd.read_csv('data/2016-08-08-last-year.xz',
                          parse_dates=[16],
                          dtype={'document_id': np.str,
                                 'congressperson_id': np.str,
                                 'congressperson_document': np.str,
                                 'term_id': np.str,
                                 'cnpj_cpf': np.str,
                                 'reimbursement_number': np.str})
    is_cnpj = dataset['cnpj_cpf'].str.len() == 14.
    cnpj_list = set(dataset.loc[is_cnpj, 'cnpj_cpf'])
    already_fetched = set(info_dataset['cnpj'].str.replace(r'[./-]', ''))
    return list(cnpj_list - already_fetched)

def raise_timeout(signum, frame):
    raise TimeoutException()

def fetch_cnpj(cnpj):
    url = 'http://receitaws.com.br/v1/cnpj/%s' % cnpj
    json_contents = urlopen(url).read().decode('utf-8')
    return json.loads(json_contents)



info_dataset = load_info_dataset()
cnpj_list = remaining_cnpjs(info_dataset)

for index, cnpj in enumerate(cnpj_list):
    print('{:,} to go'.format(len(cnpj_list) - index),
          '|',
          'Fetching %s' % cnpj)
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.alarm(1)

    try:
        info_dataset = info_dataset.append(fetch_cnpj(cnpj),
                                           ignore_index=True)
        info_dataset.to_csv(INFO_DATASET_PATH,
                            compression='xz',
                            encoding='utf-8',
                            index=False)
    except TimeoutException:
        print('Skipping due timeout')
