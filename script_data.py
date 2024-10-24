#%% Bloco 01 - importação das Libs
import os
import pandas as pd
from matplotlib import pyplot as plt
import json

#%% Bloco 02 - Dados iniciais
''' Após importação dos dados definimos um dicionario vazio, o inicio da contagem para os anos,
uma variavel para o loop, caminho dos arquivos e por fim as linhas que contem dados desnecessarios
para a analise temporal das tabelas.
'''
#Função para conversão de alguns dos dados de ppb para µg/m³ em NO2
def ppb_ugm3(ppb, massa_molar=46, volume_molar=24.45):
  return ppb * (massa_molar / volume_molar)

no2_meses = {}
ano = 2020
constante = 46/24.45
conversor = False
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
i = 1
while True:
  if ano < 2025:
  # Lê todos os dados dos anos de 2020 e 2024 devido a formatação dos dados na planilha
    if ano == 2020:
      if i < 4 and ano == 2020:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=6, dtype=str)
        conversor = True
      elif i == 6 and ano == 2020:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=3, dtype=str)
        conversor = True
      else:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=4, dtype=str)
        conversor = True
    
    # Tratamento de dados para os demais anos com pontos de parada para ano o 2021 devido ter somente 3 mêses
    elif ano >= 2021 and ano <= 2024:
      if i == 1 and ano == 2021:
        datasets = pd.read_excel(caminho + f'{ano}/Dados_SEUMA_medicao_{ano}_{i}.xlsx', usecols="A,G", skiprows=4, dtype=str)
        conversor = True
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
    if conversor:
      ''' Conversor unidades de ppb para ug/m³, 
      sendo 46 massa molar NO2 e 24.45 Volume molar a 25°c
      '''
      filtro_datasets['NO2'] = filtro_datasets['NO2'].apply(ppb_ugm3)
      remove_datasets = filtro_datasets.loc[filtro_datasets['NO2'] == 0]
      filtro_datasets.drop(remove_datasets.index, inplace=True)

    # Pula os arquivos inexistente
    if not arquivo_inexistente:
      filtro_datasets = filtro_datasets.sort_values(by='NO2')
      filtro_datasets['NO2'] = filtro_datasets['NO2'].median()
      filtro_datasets['NO2'] = filtro_datasets['NO2'].round(2)
      filtro_datasets = filtro_datasets.drop_duplicates('NO2')
      no2_meses[f'{i}_{ano-2020}'] = filtro_datasets
      arquivo_inexistente = False
      conversor = False

  # Trecho para mudança das pastas e arquivos
    i += 1
    if i == 13:
      ano += 1
      i = 1
  else:
    break

#%%
print(no2_meses['1_1'].head(5))
# mediana_condicional = no2_meses['1_2020']['NO2'].median()
# print(mediana_condicional)
#%%
union_dados = []
for mes_ano, df in no2_meses.items():

  df['Mes_ano']= mes_ano
  union_dados.append(df)

total = pd.concat(union_dados)

print(total)
#%%
plt.figure(figsize=(12,8))
# for i, key in no2_meses.items():
# mediana = total['NO2'].median()
plt.plot(total['Mes_ano'], total['NO2'], color='green', marker='o')
plt.title('2020-2024')
plt.xlabel('Ano')
plt.ylabel('NO2 (ug/m³)')
plt.tight_layout()
plt.savefig('Grafico.png', dpi=300)
plt.show()

#%% Cria um arquivo de saída com todos os daframes criados dos mêses
with open("dados.txt", "w", encoding="utf-8") as file:
    for key, df in no2_meses.items():
        file.write(f"DataFrame: {key}\n")
        file.write(df.to_csv(sep="\t", index=False, encoding="utf-8"))
        file.write("\n\n")
# %%
