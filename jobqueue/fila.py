from __future__ import annotations

import itertools
import queue
import threading
import time
from enum import Enum
from typing import Any, Callable


class StatusJob(str, Enum):
    PENDENTE = "pendente"
    EXECUTANDO = "executando"
    CONCLUIDO = "concluido"
    FALHOU = "falhou"


class Job:
    """Representa uma tarefa submetida à fila. Funciona como um 'future' simples."""

    def __init__(self, id_: int, func: Callable[..., Any], args: tuple, kwargs: dict, prioridade: int, max_tentativas: int):
        self.id = id_
        self.prioridade = prioridade
        self.max_tentativas = max_tentativas
        self.tentativas = 0
        self.status = StatusJob.PENDENTE
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._resultado: Any = None
        self._erro: BaseException | None = None
        self._terminou = threading.Event()

    def esperar(self, timeout: float | None = None) -> Any:
        """Bloqueia até o job terminar. Devolve o resultado ou relança a última exceção."""
        if not self._terminou.wait(timeout):
            raise TimeoutError(f"job {self.id} não terminou em {timeout}s")
        if self._erro is not None:
            raise self._erro
        return self._resultado

    @property
    def erro(self) -> BaseException | None:
        return self._erro

    def _finalizar(self, resultado: Any = None, erro: BaseException | None = None) -> None:
        self._resultado, self._erro = resultado, erro
        self.status = StatusJob.FALHOU if erro is not None else StatusJob.CONCLUIDO
        self._terminou.set()


class FilaDeTarefas:
    """Fila de tarefas com workers em threads, prioridades e retentativas com backoff exponencial.

    - Maior ``prioridade`` executa primeiro; empate resolvido por ordem de chegada (FIFO).
    - Uma tarefa que lança exceção é repetida até ``max_tentativas``, esperando
      ``backoff_base * 2**(tentativa-1)`` segundos entre as tentativas.
    - ``encerrar()`` deixa os jobs já enfileirados terminarem e depois para os workers.
    """

    def __init__(
        self,
        workers: int = 2,
        backoff_base: float = 0.1,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if workers < 1:
            raise ValueError("workers deve ser >= 1")
        self._fila: queue.PriorityQueue = queue.PriorityQueue()
        self._ids = itertools.count(1)
        self._ordem = itertools.count()  # desempate FIFO e evita comparar Jobs
        self._backoff_base = backoff_base
        self._sleep = sleep
        self._trava = threading.Lock()
        self._encerrada = False
        self._jobs: dict[int, Job] = {}
        self._threads = [
            threading.Thread(target=self._trabalhar, name=f"worker-{i}", daemon=True) for i in range(workers)
        ]
        for t in self._threads:
            t.start()

    # --- API pública --------------------------------------------------
    def submeter(
        self,
        func: Callable[..., Any],
        *args: Any,
        prioridade: int = 0,
        max_tentativas: int = 1,
        **kwargs: Any,
    ) -> Job:
        if max_tentativas < 1:
            raise ValueError("max_tentativas deve ser >= 1")
        with self._trava:
            if self._encerrada:
                raise RuntimeError("a fila já foi encerrada")
            job = Job(next(self._ids), func, args, kwargs, prioridade, max_tentativas)
            self._jobs[job.id] = job
            self._fila.put((-prioridade, next(self._ordem), job))
        return job

    def encerrar(self, aguardar: bool = True) -> None:
        with self._trava:
            if self._encerrada:
                return
            self._encerrada = True
            for _ in self._threads:
                # prioridade infinita: o sinal de parada fica atrás de todos os jobs reais
                self._fila.put((float("inf"), next(self._ordem), None))
        if aguardar:
            for t in self._threads:
                t.join()

    def estatisticas(self) -> dict[str, int]:
        with self._trava:
            contagem = {s.value: 0 for s in StatusJob}
            for job in self._jobs.values():
                contagem[job.status.value] += 1
            return contagem

    def __enter__(self) -> "FilaDeTarefas":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.encerrar()

    # --- workers ------------------------------------------------------
    def _trabalhar(self) -> None:
        while True:
            _, _, job = self._fila.get()
            if job is None:
                return
            self._executar(job)

    def _executar(self, job: Job) -> None:
        job.status = StatusJob.EXECUTANDO
        ultimo_erro: BaseException | None = None
        while job.tentativas < job.max_tentativas:
            job.tentativas += 1
            try:
                resultado = job._func(*job._args, **job._kwargs)
            except Exception as exc:  # noqa: BLE001 - qualquer falha da tarefa conta como tentativa
                ultimo_erro = exc
                if job.tentativas < job.max_tentativas:
                    self._sleep(self._backoff_base * 2 ** (job.tentativas - 1))
                continue
            job._finalizar(resultado=resultado)
            return
        job._finalizar(erro=ultimo_erro)
