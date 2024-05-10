# GLOBO
## Desafio técnico Globo

- Este projeto foi criado utilizando a linguagem Python em Jupyter Notebook com as biblioteca Pandas e Requests para
obtenção dos dados.

- O objetivo foi realizar a leitura de dados de uma API de com dados de Star Wars disponivel em https://swapi.dev/, 
e salvá-los em um banco de dados. O formato escolhido foi o parquet pela praticidade e e velocidade em armazenamento

- Em seguida, foi realizado a leitura desses dados utilizando a biblioteca DuckDB.

- Para execução deste projeto, siga os passos abaixo:


### Passo 1:

* Primeiro obtenha os dados, no qual pode ser feito de duas maneiras:

1.1. Pode ser feito o clone do repositorio abaixo:

```
git@github.com:cluizdes/globo.git
```

1.2. Ou ainda executar o código no Google Colab:

OBS: Para que seja efetivo rodar diretamente no Google Colab, os arquivos precisam .parquet gerados precisam ser salvos localmente e e abertos 
novamente com os arquivos Python Jupyter Notebook.

[Google Colab - Get Data Star Wars Swapi](https://colab.research.google.com/github/cluizdes/globo/blob/dev/0-sw-getdata.ipynb)
    
### Passo 2

* Realizar a leitura dos insights de cada Python Jupyter Notebook listado abaixo
[Filmes](https://colab.research.google.com/github/cluizdes/globo/blob/dev/1-sw_insights_films.ipynb)
[Personagens](https://colab.research.google.com/github/cluizdes/globo/blob/dev/2-sw_insights_people.ipynb)
[Planetas](https://colab.research.google.com/github/cluizdes/globo/blob/dev/3-sw_insights_planets.ipynb)
[Especies](https://colab.research.google.com/github/cluizdes/globo/blob/dev/4-sw_insights_species.ipynb)
[`Naves Espaciais`](https://colab.research.google.com/github/cluizdes/globo/blob/dev/5-sw_insights_starships.ipynb)
[Veiculos](https://colab.research.google.com/github/cluizdes/globo/blob/dev/6-sw_insights_vehicles.ipynb)


. Espero que este guia seja útil para guiar o projeto. Se tiver alguma dúvida, fique à vontade para perguntar.
* Cássio Luiz de Souza
@cluizdes
