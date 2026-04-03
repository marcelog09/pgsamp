# pgsamp — PostgreSQL Plugin para SA-MP / open.mp

Plugin C++17 para conectar servidores SA-MP / open.mp a bancos PostgreSQL 16 via libpq.  
Equivalente profissional ao plugin MySQL do BlueG, com suporte a prepared statements,  
transactions, cache API, async connect, pipeline mode e keepalive automático.

---

## Funcionalidades

| Categoria | Funcionalidade |
|---|---|
| Conexão | Síncrona (`pg_connect`) e assíncrona (`pg_connect_async`) |
| Queries | Async com format-string (`pg_query`) + síncrona (`pg_query_sync`) |
| Prepared Statements | `pg_prepare` / `pg_exec_prepared` / `pg_exec_prepared_sync` |
| Cache / Result API | `pg_cache_get_value`, `pg_cache_get_value_index`, row/field count |
| Field Metadata | `pg_field_name`, `pg_field_index` |
| Transactions | `pg_begin/commit/rollback` (sync + async) |
| Escaping | `pg_escape_string`, `pg_escape_literal`, `pg_escape_identifier` |
| Pipeline Mode | `pg_enter_pipeline_mode`, `pg_exit_pipeline_mode` (PostgreSQL 14+) |
| Keepalive | Thread interno automático (ping a cada 60 s) |
| Logging | Log em console + `logs/postgres.log` com timestamp |

---

## Estrutura do projeto

```
pgsamp/
├── CMakeLists.txt
├── postgres.inc                  ← include para scripts Pawn
├── src/
│   ├── main.cpp                  ← entrypoint (SAMP_SDK_IMPLEMENTATION)
│   ├── natives.cpp               ← todas as Plugin_Native exportadas
│   ├── postgres.h / .cpp         ← CallbackParam, Log, FireCallback
│   ├── postgres_internal.h       ← declaração interna de PgPlugin_RegisterFireCallback
│   ├── connection_manager.h/.cpp ← pool de conexões, keepalive thread
│   ├── result.h / .cpp           ← PgResult + ResultManager
│   └── thread_pool.h / .cpp      ← execução assíncrona (TaskType: Query/PreparedQuery/Transaction/AsyncConnect)
└── libs/
    └── samp-sdk/                 ← clonar aldergrounds/samp-sdk aqui
        └── sdk/
            └── samp_sdk.hpp
```

---

## Dependências

| Dependência | Versão mínima | Notas |
|---|---|---|
| CMake | 3.16 | |
| libpq (PostgreSQL C Client Library) | 16 | deve ser **32-bit** |
| aldergrounds/samp-sdk | latest | header-only, clonar em `libs/samp-sdk/` |
| Compilador C++ | C++17 | GCC 8+, Clang 5+, MSVC 2017+ |

---

## Compilação — Windows (MSVC)

### Pré-requisitos

1. **Visual Studio 2019 ou 2022** com componente "Desenvolvimento para Desktop com C++".
2. **CMake 3.16+** — https://cmake.org/download/
3. **PostgreSQL 16 x86 (32-bit)** — baixe o instalador de https://www.postgresql.org/download/windows/  
   Instale com a opção *command line tools* marcada, ou baixe o zip "binaries" e extraia.  
   Anote o caminho, ex.: `C:\pgsql16`.
4. Clone o SDK:
   ```
   git clone https://github.com/aldergrounds/samp-sdk.git libs/samp-sdk
   ```

### Passo a passo

```bat
REM Dentro da pasta raiz do projeto (onde fica CMakeLists.txt)
mkdir build
cd build

cmake .. -G "Visual Studio 17 2022" -A Win32 ^
  -DLIBPQ_INCLUDE_DIR="C:\pgsql16\include" ^
  -DLIBPQ_LIBRARY="C:\pgsql16\lib\libpq.lib"

cmake --build . --config Release
```

O arquivo gerado estará em:
```
build\output\Release\pgsamp.dll
```

### Deploy (Windows)

1. Copie `pgsamp.dll` para `<servidor>/plugins/`.
2. Copie `postgres.inc` para `<servidor>/pawno/include/`.
3. Copie `libpq.dll` (e suas dependências: `libssl-*.dll`, `libcrypto-*.dll`, `libintl-*.dll`)  
   do diretório `C:\pgsql16\bin` para a raiz do servidor SA-MP (onde fica `samp-server.exe`).
4. No `server.cfg` adicione:
   ```
   plugins pgsamp
   ```

---

## Compilação — Linux (GCC)

### Pré-requisitos

```bash
# Debian / Ubuntu — adicionar suporte 32-bit e instalar libpq 32-bit
sudo dpkg --add-architecture i386
sudo apt-get update
sudo apt-get install -y \
    build-essential \
    cmake \
    gcc-multilib \
    g++-multilib \
    libpq-dev:i386 \
    libssl-dev:i386

# Clone o SDK
git clone https://github.com/aldergrounds/samp-sdk.git libs/samp-sdk
```

> **CentOS/RHEL equivalente:**
> ```bash
> sudo yum install -y cmake3 glibc-devel.i686 libstdc++-devel.i686 \
>     postgresql-devel.i686 openssl-devel.i686
> ```

### Passo a passo

```bash
mkdir build && cd build

# As flags -m32 são obrigatórias (SA-MP é 32-bit)
cmake .. \
    -DCMAKE_C_FLAGS="-m32" \
    -DCMAKE_CXX_FLAGS="-m32" \
    -DCMAKE_BUILD_TYPE=Release

make -j$(nproc)
```

O arquivo gerado estará em:
```
build/output/pgsamp.so
```

### Deploy (Linux)

1. Copie `pgsamp.so` para `<servidor>/plugins/`.
2. Copie `postgres.inc` para `<servidor>/pawno/include/`.
3. No `server.cfg` adicione:
   ```
   plugins pgsamp.so
   ```
4. O `libpq.so` 32-bit precisa estar acessível ao dynamic linker. **Não basta** colocar o arquivo na raiz do servidor — o Linux não busca no diretório atual. Use uma das opções abaixo:

   **Opção A — via sistema (recomendada):** instale o pacote `libpq-dev:i386` (já feito na etapa de pré-requisitos). O `.so` ficará em `/usr/lib/i386-linux-gnu/` e será encontrado automaticamente.

   **Opção B — via `LD_LIBRARY_PATH`:** se a biblioteca estiver em um diretório personalizado, exporte a variável antes de iniciar o servidor:
   ```bash
   export LD_LIBRARY_PATH=/caminho/para/libpq:$LD_LIBRARY_PATH
   ./samp03svr
   ```

   **Opção C — via `ldconfig`:** copie o `.so` para `/usr/lib/i386-linux-gnu/` e rode:
   ```bash
   sudo ldconfig
   ```

---

## Uso no Pawn

### Conexão básica

```pawn
#include <postgres>

new g_conn;

public OnGameModeInit() {
    g_conn = pg_connect("127.0.0.1", "root", "senha", "game", 5432);
    if (g_conn == PG_INVALID_HANDLE) {
        print("[PG] Falha ao conectar!");
        return 0;
    }
    return 1;
}

public OnGameModeExit() {
    pg_close(g_conn);
    return 1;
}
```

### Conexão assíncrona

```pawn
public OnGameModeInit() {
    pg_connect_async("127.0.0.1", "root", "senha", "game", 5432, "OnPgConnected");
    return 1;
}

forward OnPgConnected(connection_handle);
public  OnPgConnected(connection_handle) {
    if (!connection_handle) { print("[PG] Conexão assíncrona falhou!"); return; }
    g_conn = connection_handle;
    print("[PG] Conectado com sucesso (async).");
}
```

### Query assíncrona com parâmetros no callback

```pawn
// Passa playerid extra para o callback (formato 'd' = inteiro)
pg_query(g_conn,
         "SELECT nome, score FROM jogadores",
         "OnPlayersLoaded",
         "d",
         playerid);

PG_CALLBACK_EX(OnPlayersLoaded, playerid) {
    new rows = pg_cache_get_row_count(result_handle);
    for (new i = 0; i < rows; i++) {
        new nome[64];
        pg_cache_get_value(result_handle, i, "nome", nome);
        printf("player %d -> %s", playerid, nome);
    }
    pg_free_result(result_handle);
}
```

### Query síncrona + Cache API

```pawn
new res = pg_query_sync(g_conn, "SELECT id, nome FROM jogadores LIMIT 10");
if (res == PG_INVALID_RESULT) return;

new rows  = pg_cache_get_row_count(res);
new fields= pg_cache_get_field_count(res);
printf("Linhas: %d | Colunas: %d", rows, fields);

new nome[64];
for (new i = 0; i < rows; i++) {
    pg_cache_get_value(res, i, "nome", nome);
    printf("  [%d] %s", i, nome);
}

pg_free_result(res);
```

### Prepared Statements

```pawn
// Registrar (uma vez, ex. no OnGameModeInit)
pg_prepare(g_conn, "sel_jogador",
           "SELECT nome, score FROM jogadores WHERE id = 1");

// Executar de forma assíncrona com extra param
pg_exec_prepared(g_conn, "sel_jogador", "OnJogadorCarregado", "d", playerid);

PG_CALLBACK_EX(OnJogadorCarregado, playerid) {
    new nome[64];
    pg_cache_get_value(result_handle, 0, "nome", nome);
    printf("Jogador %d: %s", playerid, nome);
    pg_free_result(result_handle);
}

// Executar de forma síncrona
new res = pg_exec_prepared_sync(g_conn, "sel_jogador");
pg_free_result(res);
```

### Field Metadata

```pawn
new res = pg_query_sync(g_conn, "SELECT id, nome, score FROM jogadores LIMIT 1");

// Nome da coluna pelo índice
new col_name[64];
pg_field_name(res, 0, col_name);       // col_name = "id"
pg_field_name(res, 1, col_name);       // col_name = "nome"

// Índice da coluna pelo nome
new idx = pg_field_index(res, "score"); // idx = 2

pg_free_result(res);
```

### Transactions

```pawn
// Síncrona
pg_begin(g_conn);
pg_query_sync(g_conn, "UPDATE contas SET saldo = saldo - 100 WHERE id = 1");
pg_query_sync(g_conn, "UPDATE contas SET saldo = saldo + 100 WHERE id = 2");
pg_commit(g_conn);

// Assíncrona encadeada
pg_begin_async(g_conn, "OnTxIniciada");

forward OnTxIniciada(result_handle);
public  OnTxIniciada(result_handle) {
    if (!result_handle) { print("BEGIN falhou"); return; }
    pg_query(g_conn, "UPDATE contas SET saldo = saldo - 50 WHERE id = 3",
             "OnUpdateFeito");
}

PG_CALLBACK(OnUpdateFeito) {
    pg_commit_async(g_conn);
    pg_free_result(result_handle);
}
```

### Escape avançado

```pawn
new safe_val[256], safe_id[128];

// Escapa valor para interpolação — inclui aspas simples
pg_escape_literal(g_conn, player_name, safe_val);
// safe_val = 'O''Brien'

// Escapa identificador (nome de tabela/coluna) — inclui aspas duplas
pg_escape_identifier(g_conn, table_name, safe_id);
// safe_id = "jogadores"

new query[512];
format(query, sizeof query,
       "SELECT * FROM %s WHERE nome = %s", safe_id, safe_val);
pg_query(g_conn, query, "OnResultado");
```

### pg_format — formatação segura de queries

`pg_format` funciona como `mysql_format`: monta a query já com os valores escapados,  
sem precisar de chamadas separadas a `pg_escape_literal`.

**Especificadores:**

| Especificador | Tipo | Comportamento |
|---|---|---|
| `%d` / `%i` | inteiro | Inserido diretamente |
| `%f` | float | Inserido diretamente |
| `%s` | string | **Sem escape** — use só com valores confiáveis |
| `%e` | string | `PQescapeLiteral` — envolve em aspas simples (`'valor'`) |
| `%E` | string | `PQescapeIdentifier` — envolve em aspas duplas (`"tabela"`) |
| `%%` | literal | Insere um `%` |

```pawn
// INSERT com escape automático de strings (recomendado)
new query[512];
pg_format(g_conn, query, sizeof query,
    "INSERT INTO chat_log (nick, conta_id, mensagem, momento) "
    "VALUES (%e, %d, %e, TO_TIMESTAMP(%d))",
    P_Nome[playerid],
    PlayerInfo[playerid][IDConta],
    texto,
    gettime()
);
pg_query(g_conn, query, "");

// Nome de tabela dinâmico via %E (escape de identificador)
new tabela[] = "jogadores", busca[] = "O'Brien";
pg_format(g_conn, query, sizeof query,
    "SELECT * FROM %E WHERE nome = %e",
    tabela, busca
);
// resultado: SELECT * FROM "jogadores" WHERE nome = 'O''Brien'
```

### Pipeline Mode (PostgreSQL 14+)

```pawn
// Ativar pipeline — múltiplas queries sem esperar cada resultado
pg_enter_pipeline_mode(g_conn);

pg_query(g_conn, "INSERT INTO log VALUES ('evento1')", "OnInsert1");
pg_query(g_conn, "INSERT INTO log VALUES ('evento2')", "OnInsert2");
pg_query(g_conn, "INSERT INTO log VALUES ('evento3')", "OnInsert3");

// Sair do pipeline (após consumir todos os resultados)
pg_exit_pipeline_mode(g_conn);
```

---

## Referência de Natives

### Conexão
| Native | Descrição |
|---|---|
| `pg_connect(host, user, pass, db, port)` | Abre conexão síncrona. Retorna handle > 0 ou `PG_INVALID_HANDLE`. |
| `pg_close(conn)` | Fecha e libera conexão. |
| `pg_connect_async(host, user, pass, db, port, callback)` | Abre conexão em background. Callback: `(connection_handle)`. |

### Queries
| Native | Descrição |
|---|---|
| `pg_query(conn, query, callback, format="", ...)` | Query assíncrona. Parâmetros extras (`d`/`f`/`s`) são passados ao callback. |
| `pg_query_sync(conn, query)` | Query síncrona. Retorna result handle. |
| `pg_free_result(result)` | Libera result set. |

### Result / Cache
| Native | Descrição |
|---|---|
| `pg_num_rows(result)` | Número de linhas. |
| `pg_num_fields(result)` | Número de colunas. |
| `pg_get_field(result, row, col_index, dest[], size)` | Campo por índice. |
| `pg_cache_get_value(result, row, col_name[], dest[], size)` | Campo por nome. |
| `pg_cache_get_value_index(result, row, col_index, dest[], size)` | Campo por índice (alias). |
| `pg_cache_get_row_count(result)` | Número de linhas (alias). |
| `pg_cache_get_field_count(result)` | Número de colunas (alias). |

### Field Metadata
| Native | Descrição |
|---|---|
| `pg_field_name(result, col_index, dest[], size)` | Nome da coluna pelo índice. |
| `pg_field_index(result, col_name[])` | Índice da coluna pelo nome. Retorna -1 se não encontrado. |

### Prepared Statements
| Native | Descrição |
|---|---|
| `pg_prepare(conn, name[], query[])` | Registra prepared statement no servidor. |
| `pg_exec_prepared(conn, name[], callback, format="", ...)` | Executa async. |
| `pg_exec_prepared_sync(conn, name[])` | Executa síncrono. Retorna result handle. |

### Transactions
| Native | Descrição |
|---|---|
| `pg_begin(conn)` | BEGIN síncrono. |
| `pg_commit(conn)` | COMMIT síncrono. |
| `pg_rollback(conn)` | ROLLBACK síncrono. |
| `pg_begin_async(conn, callback="")` | BEGIN assíncrono. |
| `pg_commit_async(conn, callback="")` | COMMIT assíncrono. |
| `pg_rollback_async(conn, callback="")` | ROLLBACK assíncrono. |

### Escaping
| Native | Descrição |
|---|---|
| `pg_escape_string(conn, input, output[], size)` | Escape básico, sem aspas. |
| `pg_escape_literal(conn, input, output[], size)` | Escape com aspas simples (`'valor'`). |
| `pg_escape_identifier(conn, input, output[], size)` | Escape com aspas duplas (`"tabela"`). |
| `pg_format(conn, output[], size, format[], ...)` | Formata query com escape embutido. Especificadores: `%d` `%f` `%s` `%e` (literal) `%E` (identificador) `%%`. |

### Pipeline
| Native | Descrição |
|---|---|
| `pg_enter_pipeline_mode(conn)` | Ativa pipeline mode (libpq 14+). |
| `pg_exit_pipeline_mode(conn)` | Desativa pipeline mode. |

---

## Macros Pawn

| Macro | Expansão |
|---|---|
| `PG_CALLBACK(Nome)` | `forward Nome(result_handle); public Nome(result_handle)` |
| `PG_CALLBACK_EX(Nome, params)` | `forward Nome(result_handle, params); public Nome(result_handle, params)` |

---

## Constantes

| Constante | Valor | Uso |
|---|---|---|
| `PG_INVALID_HANDLE` | `0` | Retornado por `pg_connect` em falha |
| `PG_INVALID_RESULT` | `0` | Retornado por `pg_query_sync` em falha |

---

## Logs

O plugin grava em `logs/plugins/postgres.log` (relativo à pasta do servidor):

```
[2026-04-02 10:00:01] [INFO] Connected to PostgreSQL (handle 1): host=127.0.0.1 db=meu_banco
[2026-04-02 10:00:05] [ERROR] pg_query_sync error (handle 1): ERROR:  column "inexistente" does not exist
[2026-04-02 10:00:10] [WARN] Connection 1 lost. Attempting reconnection...
```

---

## Segurança

- **Nunca** construa queries concatenando strings diretamente. Use `pg_format` com `%e`/`%E` (recomendado) ou `pg_escape_literal`/`pg_escape_identifier` separadamente.
- O plugin **não armazena** credenciais após a conexão ser estabelecida; a string de conexão fica apenas dentro do `PQconnectdb` call.
- Toda comunicação entre threads e main thread usa filas protegidas por mutex — sem corridas de dados.
