#%% Bloco 01 - importação das Libs
import os
import pandas as pd
import matplotlib as mpl
import json

#%% Bloco 02 - Dados iniciais
''' Após importação dos dados definimos um dicionario vazio, o inicio da contagem para os anos,
uma variavel para o loop, caminho dos arquivos e por fim as linhas que contem dados desnecessarios
para a analise temporal das tabelas.
'''
no2_meses = {}
ano = 2020
i = 1
arquivo_inexistente = False
caminho = os.getcwd() + f'/datasets/'
excluir_linhas = [ 
   'FALHA OPERAÇÃO',
   'FALHA DE OPERAÇÃO',
   'FORÇA MAIOR',
   'MANUTENÇÃO',
   'CALIBRAÇÃO',
   'DADOS VÁLIDOS',
   'EFICIÊNCIA DO DIA',
   'Dados para o mês:',
   'mínimo',
   'máximo',
   'média',
   'soma (acumulado)',
   'soma',
   'Minímo',
   'Máximo',
   'Média',
   'Soma',
   'UNIDADES',
   'MÊS',
   '0',
   '-9999.=',
   '3.64=',
   'DIA',
   '',
   '10%',
   '1.65x'
   ]

#%% Bloco 03 - Leitura, mineração dos dados e exportação
while True:
  if ano < 2025:
  # Lê todos os dados dos anos de 2020 e 2024 devido a formatação dos dados na planilha
    if ano == 2020:
      if i < 4 and ano == 2020:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=6, dtype=str)
      elif i == 6 and ano == 2020:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=3, dtype=str)
      else:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=4, dtype=str)
    
    # Tratamento de dados para os demais anos com pontos de parada para ano o 2021 devido ter somente 3 mêses
    elif ano >= 2021 and ano <= 2024:
      if i == 1 and ano == 2021:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,D", skiprows=4, dtype=str)
      elif (i == 11 or i == 12) and ano == 2021:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,D", skiprows=3, dtype=str)
        arquivo_inexistente = False
      else:
        try:
          datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,D", skiprows=3, dtype=str)
          arquivo_inexistente = False
        except FileNotFoundError as e:
           print(f'Não foi encontrado o arquivo {i}/{ano}: {e}')
           arquivo_inexistente = True
        except TypeError as e:
          print(f'Erro ao ler o arquivo {i}/{ano}: {e}')
          arquivo_inexistente = True

  # Verifica e limpa os dados dos datasets e inseri em um dicionário
  if not ano == 2025:
    datasets.replace(['', ' ', 'N/A', 'NULL'],pd.NA, inplace=True)
    datasets.columns = ['dia', 'NO2']
    datasets['NO2'] = datasets['NO2'].str.replace('<','', regex=True)
    datasets['NO2'] = datasets['NO2'].str.replace('>','', regex=True).str.replace('f', '', regex=True)
    datasets['NO2'] = datasets['NO2'].str.replace(',','.', regex=True)
    datasets = datasets.dropna()
    filtro_datasets = datasets[~datasets.isin(excluir_linhas).any(axis=1)]
    filtro_datasets['NO2'] = filtro_datasets['NO2'].astype(float)

    # Pula os arquivos inexistente
    if not arquivo_inexistente:
      no2_meses[f'{i}_{ano}'] = filtro_datasets
      arquivo_inexistente = False

  # Trecho para mudança das pastas e arquivos
    i += 1
    if i == 13:
      ano += 1
      i = 1
  else:
    break

# Cria um arquivo de saída com todos os daframes criados dos mêses
with open("dados.txt", "w", encoding="utf-8") as file:
    for key, df in no2_meses.items():
        file.write(f"DataFrame: {key}\n")
        file.write(df.to_csv(sep="\t", index=False, encoding="utf-8"))
        file.write("\n\n")
# %%
