### REDES
## TRABALHO II

# Descrição

Queremos implementar um sistema de compartilhamento de arquivos P2P e comparar o desempenho dele com uma abordagem tradicional cliente-servidor. Na prática, queremos quebrar o arquivo original em blocos fixos e mapealos em um manifesto (que guarda o SHA-256). Para os nós se encontrarem na rede, temos um tracker (HTTP) simples que controla quem está online: cada peer manda um "heartbeat" a cada intervalo de tempo e se sumir é removido da lista.

A troca de blocos acontece via TCP. Os peers se conectam, avisam quais partes já têm e começam a pedir blocos diferentes para vários vizinhos ao mesmo tempo. Antes de salvar qualquer pacote no disco, o nó bate o hash do bloco recebido com o manifesto, e se vier corrompido, o peer que mandou entra numa lista de banidos.

Para manter o comparativo justo, o mesmo código também roda em modo cliente-servidor (onde todo mundo só baixa do nó inicial e ninguém repassa nada para frente), mantendo exatamente os mesmos limites de banda.

Escopo :
- Descoberta de pares, seja por um tracker simples ou por broadcast/multicast na rede local.
- Arquivos divididos em blocos de tamanho fixo, com download paralelo de blocos vindos de pares diferentes.
- Verificação de integridade de cada bloco por hash (SHA-256), com o arquivo descrito por um manifesto.
- Atualização dinâmica da lista de pares ativos, com detecção de saída por heartbeat.
- Desempenho: política "mais raro primeiro" (rarest-first) versus sequencial, medindo o efeito na disponibilidade do arquivo quando a semente original sai.
- Benchmark na prática: P2P vs. Cliente-Servidor

# Possíveis Melhorias Futuras
- Desempenho: incentivo ao compartilhamento (tit-for-tat), demonstrando que um free-rider passa a baixar mais devagar.
- Desempenho: substituir o tracker por uma DHT (Kademlia simplificada) e medir quantos saltos uma busca leva conforme a rede cresce.
- Segurança: árvore de Merkle no manifesto e manifesto assinado pelo publicador, de modo que um par malicioso seja detectado e banido automaticamente.
