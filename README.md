# MiniTorrent

## Descrição

O **MiniTorrent** é uma implementação educacional de um sistema de compartilhamento de arquivos ponto a ponto (P2P) inspirado no protocolo BitTorrent, desenvolvido do zero em Go. O projeto permite comparar experimentalmente, na mesma base de código, o desempenho e a resiliência da arquitetura **P2P** frente ao modelo tradicional **Cliente-Servidor (CS)**.

## Tecnologias Utilizadas

- **Linguagem de programação:** Go (versão 1.21 ou superior).
- **Bibliotecas e frameworks:** Apenas a **Biblioteca Padrão do Go** (`net`, `net/http`, `crypto/sha256`, `encoding/binary`, `encoding/json`, `context`, `sync` e `sync/atomic`).
- **Automação e análise de experimentos:** GNU Make, Bash, `sha256sum`, `dd`, `curl` e `jq`.

## Como Executar

### Requisitos

- **Go 1.21+** instalado e disponível no `PATH` (`go version`).
- **Linux/macOS** com `make`, `bash`, `dd`, `sha256sum`.

### Instruções de Execução

1. Clone o repositório e acesse a pasta do projeto:
   ```
   bash
   git clone https://github.com/Matheus00082/REDES.git
   cd REDES
   ```

2. Verifique o código e compile o binário (não há dependências externas para instalar):
   ```
   bash
   go vet ./...
   go build -race -o minitorrent .
   ```

3. Gere um arquivo de exemplo (ou utilize um arquivo existente) e crie o seu manifesto JSON com os hashes SHA-256 dos blocos:
   ```
   bash
   dd if=/dev/urandom of=teste.bin bs=1M count=16
   ./minitorrent manifest -in teste.bin -out teste.manifest.json
   ```

4. Inicie o **tracker** (servidor de descoberta) em um primeiro terminal:
   ```
   bash
   ./minitorrent tracker -listen 127.0.0.1:8000
   ```

5. Inicie a **semente (`seed`)** em um segundo terminal (opcionalmente aplicando `-up-limit` em bytes/s para simular limitação de banda):
   ```
   bash
   ./minitorrent seed -id seed -manifest teste.manifest.json -file teste.bin -up-limit 2097152
   ```

6. Execute um ou mais **clientes (`get`)** em outros terminais para baixar o arquivo:
   - **Modo P2P (padrão, com seleção *rarest-first*):**
     ```
     bash
     ./minitorrent get -id peer-1 -manifest teste.manifest.json -out copia-1.bin -mode p2p -picker rarest -linger -result peer-1.json
     ```
   - **Modo Cliente-Servidor (baixa apenas da semente, sem repassar blocos):**
     ```
     bash
     ./minitorrent get -id cs-1 -manifest teste.manifest.json -out cs-1.bin -mode cs -result cs-1.json
     ```
     
## Como Testar

O projeto inclui um `Makefile` pronto para validar a compilação, a ausência de condições de corrida (`-race`), a integridade dos dados e os quatro cenários experimentais de forma automatizada.

### Execução completa da bateria de demonstração

```
bash
make demo
```

Esse comando executa sequencialmente:
1. **Compilação e validação:** `go vet ./...` e `go build -race`.
2. **Cenário A vs. Cenário B (`make compare`):** compara 4 clientes baixando 16 MiB de uma semente limitada a 2 MiB/s em modo **Cliente-Servidor (`cs`)** versus **P2P (`rarest-first`)**, exibindo tempos individuais, *makespan*, percentual de tráfego entre peers, ganho de velocidade (*speedup*) e conferência via `sha256sum`.
3. **Cenário C (`make demo-corrupt`):** insere uma semente maliciosa (`-corrupt 1.0`) ao lado da semente legítima e comprova a rejeição de blocos adulterados, o banimento automático após 3 falhas e a integridade final do arquivo.
4. **Cenário D (`make demo-leave`):** configura a semente para abandonar o enxame após enviar `1.2x` o tamanho do arquivo (`-leave-after 1.2`) e comprova que os peers completam 100% do arquivo entre si.

### Outros alvos úteis do `Makefile`

```
bash
make help                                         # Lista todos os comandos e variáveis configuráveis
make compare SIZE_MB=32 PEERS=6 UP_LIMIT=1048576  # Personaliza tamanho, quantidade de peers e banda
make report                                       # Reexibe a tabela comparativa a partir dos JSONs em demo_out/
make swarms                                       # Consulta os enxames ativos no tracker (http://127.0.0.1:8000/swarms)
make clean                                        # Remove o binário compilado e a pasta demo_out/
```

## Funcionalidades Implementadas

- **Gerador e validador de manifesto (`internal/manifest`):** divisão de arquivos em blocos configuráveis (padrão de 256 KiB, limitado ao máximo do protocolo), cálculo de SHA-256 por bloco, validação de coerência na leitura e cálculo do `InfoHash`.
- **Tracker HTTP com heartbeat e TTL (`internal/tracker`):** endpoints `/announce` (POST e GET) e `/swarms` (GET) para descoberta de peers, remoção automática de nós inativos, suporte ao campo `mode` e proteção de limites/timeouts no servidor HTTP.
- **Protocolo binário sobre TCP (`internal/wire`):** *handshake* mútuo validando `InfoHash` e `peerID`, enquadramento seguro de mensagens (`KEEPALIVE`, `BITFIELD`, `HAVE`, `REQUEST`, `PIECE`) e manipulação compacta de *bitfields* (1 bit por bloco).
- **Motor P2P e Cliente-Servidor (`internal/peer`):**
  - Download paralelo com *pipeline* de até 8 requisições por vizinho e prevenção de conexões duplicadas.
  - Políticas intercambiáveis de seleção de blocos (`rarest` com sorteio uniforme entre empatados e `sequential`) desacopladas em `picker.go`.
  - Verificação SHA-256 de cada bloco recebido e banimento automático após 3 blocos corrompidos (`-corrupt` para simulação).
  - Detecção de pedidos travados (*timeout* de *inflight*) com redistribuição de blocos e desconexão de vizinhos que não respondem.
  - Armazenamento com escrita aleatória em disco (`ReadAt`/`WriteAt`) e retomada automática de downloads parciais.
  - Encerramento gracioso (*graceful shutdown*), modo `-linger`, saída programada por volume enviado (`-leave-after`) e exportação atômica de métricas em JSON (`-result`).
- **Limitador de banda (`internal/ratelimit`):** controle de taxa baseado em *token bucket* integrado ao upload (`-up-limit`) e ao download (`-down-limit`) dos peers.

## Possíveis Melhorias Futuras

- **Modo *Endgame*:** enviar requisições redundantes para os últimos blocos pendentes do arquivo a múltiplos vizinhos simultaneamente, reduzindo o impacto de peers lentos na reta final do download.
- **Controle de reciprocidade (*Choke/Unchoke* e *Tit-for-Tat*):** priorizar o envio de blocos para os vizinhos que mais contribuem com upload no enxame, desestimulando *free-riders*.
- **Descoberta descentralizada (DHT / PEX):** permitir que peers encontrem novos vizinhos sem depender de um tracker centralizado.
- **Suíte de testes automatizados em Go (`*_test.go`):** adicionar testes unitários e de integração executáveis via `go test -race ./...` para todos os pacotes internos.
- **Criptografia de transporte (TLS) e suporte a diretórios:** proteger a comunicação TCP entre peers e estender o formato do manifesto para compartilhar múltiplos arquivos em um único enxame.
