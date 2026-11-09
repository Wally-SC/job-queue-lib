# job-queue-lib

Biblioteca pequena de **fila de tarefas em background**, só com a biblioteca padrão
do Python: workers em threads, prioridades, retentativas com backoff exponencial e
encerramento ordenado.

## Exemplo

```python
from jobqueue import FilaDeTarefas

def baixar(url):
    ...

with FilaDeTarefas(workers=4, backoff_base=0.5) as fila:
    job = fila.submeter(baixar, "https://exemplo.com", prioridade=10, max_tentativas=5)
    resultado = job.esperar(timeout=30)   # relança a exceção se todas as tentativas falharem
    print(fila.estatisticas())            # {'pendente': 0, 'executando': 0, 'concluido': 1, 'falhou': 0}
```

## Comportamento

| Tema | Regra |
|---|---|
| Ordem | maior `prioridade` primeiro; empate resolvido por ordem de chegada (FIFO) |
| Retentativa | até `max_tentativas`; espera `backoff_base * 2**(n-1)` entre tentativas (ex.: 0,5s, 1s, 2s) |
| Falha definitiva | `job.esperar()` relança a última exceção; o worker continua vivo |
| Encerramento | `encerrar()` deixa os jobs já enfileirados terminarem e só então para os workers |
| Após encerrar | `submeter()` levanta `RuntimeError` |

## Detalhes de implementação

- `queue.PriorityQueue` com chave `(-prioridade, contador)`; o contador garante FIFO no
  empate e evita comparar objetos `Job`.
- O sinal de parada entra na fila com prioridade infinita, ficando **atrás** de todos os jobs reais.
- O `sleep` é injetável, então os testes de backoff rodam instantaneamente e verificam
  os tempos exatos (`[1.0, 2.0]`) sem depender do relógio.

## Limitações conhecidas

- O backoff **bloqueia o worker** durante a espera. Uma melhoria é reenfileirar o job
  com um horário de execução (heap de "prontos em").
- Threads: bom para tarefas de I/O; tarefas de CPU pesadas pedem processos.
- Sem persistência: jobs pendentes se perdem se o processo cair.

## Testes

```bash
pip install pytest
pytest
```
