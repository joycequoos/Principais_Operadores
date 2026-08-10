# Principais Operadores do Airflow

[← Voltar à Trilha de Airflow](https://github.com/joycequoos/Apache_Airflow./blob/main/README.md)

<!--
  Comentário: assim como no README de Parâmetros, este é uma lista de
  referência, não um tutorial passo a passo — por isso organizei por
  FUNÇÃO em vez de ordem cronológica: operadores de execução genérica →
  integração/notificação → sensores e organização. O original tinha só
  uma linha de definição por operador, sem exemplo de import ou uso —
  adicionei um trecho de código para cada um.
-->

Um **Operator** define *o que* uma task faz dentro de uma DAG. O Airflow já vem com uma boa variedade de operadores prontos — abaixo, os principais, organizados por função.

---

## Operadores de execução genérica

Os operadores mais usados no dia a dia, para rodar comandos ou código Python.

**1. `BashOperator`** — executa um comando de shell ou script.

```python
from airflow.operators.bash import BashOperator

task = BashOperator(task_id="tsk1", bash_command="echo 'olá mundo'", dag=dag)
```

**2. `PythonOperator`** — executa uma função Python.

```python
from airflow.operators.python import PythonOperator

def minha_funcao():
    print("executando em Python")

task = PythonOperator(task_id="tsk2", python_callable=minha_funcao, dag=dag)
```

---

## Operadores de integração e notificação

Operadores que conectam a DAG a sistemas externos (e-mail, bancos de dados, outras DAGs).

**3. `EmailOperator`** — envia um e-mail.

```python
from airflow.operators.email import EmailOperator

task = EmailOperator(
    task_id="enviar_email",
    to="destinatario@email.com",
    subject="Pipeline concluído",
    html_content="A DAG foi executada com sucesso.",
    dag=dag
)
```

**4. Operadores de SQL** — executam uma consulta SQL em um banco de dados.

<!--
  Comentário: "SQLOperator" genérico, do jeito que estava no original,
  não existe como tal no Airflow — corrigi para os nomes reais, que
  variam de acordo com o banco de dados utilizado.
-->

> ⚠️ Não existe um `SQLOperator` genérico no Airflow — o nome do operador muda de acordo com o banco de dados: `PostgresOperator`, `MySqlOperator`, `SqliteOperator`, `MsSqlOperator`, entre outros, cada um exigindo a *connection* correspondente configurada no Airflow.

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

task = PostgresOperator(
    task_id="consulta_sql",
    postgres_conn_id="meu_banco",
    sql="SELECT * FROM tabela;",
    dag=dag
)
```

**5. `TriggerDagRunOperator`** — dispara a execução de outra DAG.

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

task = TriggerDagRunOperator(
    task_id="disparar_outra_dag",
    trigger_dag_id="nome_da_dag_filha",
    dag=dag
)
```

📖 Já vimos esse operador em detalhes no README [DAG executa DAG](https://github.com/joycequoos/Dag_Executa_Dag).

---

## Sensores e organização

**Sensores** são um tipo especial de operador: eles ficam "esperando" até que uma condição externa seja satisfeita, antes de deixar a DAG continuar.

**6. `FileSensor`** — aguarda até que um arquivo seja criado ou modificado.

```python
from airflow.sensors.filesystem import FileSensor

task = FileSensor(
    task_id="aguardar_arquivo",
    filepath="/caminho/arquivo.csv",
    poke_interval=30,
    dag=dag
)
```

**7. `HttpSensor`** — aguarda até que uma requisição HTTP seja bem-sucedida.

```python
from airflow.providers.http.sensors.http import HttpSensor

task = HttpSensor(
    task_id="aguardar_api",
    http_conn_id="minha_api",
    endpoint="status",
    dag=dag
)
```

**8. `DummyOperator`** — não faz nada; útil para fins de organização e sincronização de tasks.

```python
from airflow.operators.dummy_operator import DummyOperator

task = DummyOperator(task_id="ponto_de_sincronizacao", dag=dag)
```

> 💡 Em versões mais recentes do Airflow (2.4+), prefira o `EmptyOperator` (`airflow.operators.empty.EmptyOperator`) — o `DummyOperator` está obsoleto. Já exploramos esse operador em detalhes no README [DAG com Task Dummy](https://github.com/joycequoos/Dags_Exercicios).

---

Com os principais operadores mapeados, o próximo passo é entender as regras de gatilho — que definem em quais condições uma task deve (ou não) ser executada.

➡️ **Próximo passo:** [Regras de Gatilho](https://github.com/JosiTubaroski/Regras_Gatilho/blob/main/README.md)
