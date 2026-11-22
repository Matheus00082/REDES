SHELL := /bin/bash
.ONESHELL:
.DEFAULT_GOAL := help

# ==============================================================================
# MiniTorrent — Automação de compilação e demonstração (CS vs P2P, falhas, churn)
# ==============================================================================
# Você pode sobrescrever qualquer parâmetro na linha de comando, por exemplo:
#   make demo SIZE_MB=32 PEERS=4 UP_LIMIT=2097152
#   make compare SIZE_MB=16 PEERS=4 UP_LIMIT=1048576

BIN          ?= ./minitorrent
DEMO_DIR     ?= demo_out
TEST_FILE    ?= $(DEMO_DIR)/teste.bin
MANIFEST     ?= $(DEMO_DIR)/teste.manifest.json
SIZE_MB      ?= 16
PEERS        ?= 4
UP_LIMIT     ?= 2097152
LEAVE_AFTER  ?= 1.2
PICKER_LEAVE ?= 1
PICKER_WAIT  ?= 10
REPS         ?= 3
TRACKER_ADDR ?= 127.0.0.1:8000
TRACKER_URL  ?= http://$(TRACKER_ADDR)
TIMEOUT_SEC  ?= 180

.PHONY: help build prepare demo demo-cs demo-p2p report compare demo-corrupt demo-leave demo-picker swarms clean

help:
	@printf '%s\n' \
		"MiniTorrent - Comandos disponíveis:" \
		"" \
		"  make demo          Executa toda a demonstração (build, CS vs P2P, malicioso e saída da semente)" \
		"  make compare       Executa CS e P2P com o mesmo limite de upload em todos os nós e imprime o comparativo" \
		"  make report        Exibe a tabela comparativa dos últimos resultados salvos em $(DEMO_DIR)/" \
		"  make demo-cs       Executa apenas o cenário Cliente-Servidor (-mode cs)" \
		"  make demo-p2p      Executa apenas o cenário P2P com rarest-first (-mode p2p)" \
		"  make demo-corrupt  Demonstra detecção SHA-256 e banimento automático de peer malicioso" \
		"  make demo-leave    Demonstra sobrevivência do enxame após saída precoce da semente (-leave-after)" \
		"  make demo-picker   Compara rarest-first e sequencial: quanto do arquivo sobra quando a semente sai" \
		"  make build         Roda go vet e compila $(BIN) com detector de race (-race)" \
		"  make prepare       Gera arquivo aleatório ($(SIZE_MB) MiB) e manifesto SHA-256" \
		"  make swarms        Consulta os enxames ativos no tracker em $(TRACKER_URL)/swarms" \
		"  make clean         Remove $(DEMO_DIR)/ e o binário $(BIN)" \
		"" \
		"Parâmetros configuráveis (valores atuais):" \
		"  SIZE_MB=$(SIZE_MB)  PEERS=$(PEERS)  UP_LIMIT=$(UP_LIMIT) B/s  LEAVE_AFTER=$(LEAVE_AFTER)  TRACKER_ADDR=$(TRACKER_ADDR)" \
		"  PICKER_LEAVE=$(PICKER_LEAVE)  PICKER_WAIT=$(PICKER_WAIT) s  REPS=$(REPS)"

build:
	@set -euo pipefail
	echo "==> [1/2] Validando código (go vet) e compilando $(BIN) com -race..."
	go vet ./...
	go build -race -o "$(BIN)" .
	echo "    Binário pronto: $(BIN)"

prepare: build
	@set -euo pipefail
	mkdir -p "$(DEMO_DIR)"
	expected_bytes=$$(( $(SIZE_MB) * 1024 * 1024 ))
	current_bytes=0
	if [ -f "$(TEST_FILE)" ]; then
		current_bytes=$$(wc -c < "$(TEST_FILE)")
	fi
	if [ "$$current_bytes" -ne "$$expected_bytes" ] || [ ! -f "$(MANIFEST)" ]; then
		echo "==> [2/2] Gerando arquivo de teste ($(SIZE_MB) MiB) e manifesto SHA-256..."
		dd if=/dev/urandom of="$(TEST_FILE)" bs=1M count="$(SIZE_MB)" status=none
		"$(BIN)" manifest -in "$(TEST_FILE)" -out "$(MANIFEST)"
	else
		echo "==> [2/2] Reutilizando $(TEST_FILE) ($(SIZE_MB) MiB) e $(MANIFEST) já existentes."
	fi

demo-cs: prepare
	@set -euo pipefail
	echo ""
	echo "========================================================================"
	echo " CENÁRIO A: CLIENTE-SERVIDOR (-mode cs | $(PEERS) clientes | semente $(UP_LIMIT) B/s)"
	echo "========================================================================"
	rm -f "$(DEMO_DIR)"/cs-*.bin "$(DEMO_DIR)"/cs-*.json "$(DEMO_DIR)"/cs-*.log

	PIDS=()
	cleanup() {
		for pid in "$${PIDS[@]:-}"; do
			kill "$$pid" 2>/dev/null || true
		done
		wait 2>/dev/null || true
	}
	trap cleanup EXIT INT TERM

	"$(BIN)" tracker -listen "$(TRACKER_ADDR)" >"$(DEMO_DIR)/cs-tracker.log" 2>&1 &
	TRACKER_PID=$$!
	PIDS+=("$$TRACKER_PID")

	ready=0
	for _ in $$(seq 1 40); do
		if ! kill -0 "$$TRACKER_PID" 2>/dev/null; then
			echo "ERRO: tracker encerrou inesperadamente:"
			cat "$(DEMO_DIR)/cs-tracker.log"
			exit 1
		fi
		if command -v curl >/dev/null 2>&1; then
			if curl -sf "$(TRACKER_URL)/swarms" >/dev/null 2>&1; then
				ready=1
				break
			fi
		else
			sleep 0.5
			ready=1
			break
		fi
		sleep 0.1
	done
	if [ "$$ready" -ne 1 ]; then
		echo "ERRO: tracker não respondeu em $(TRACKER_URL)"
		cat "$(DEMO_DIR)/cs-tracker.log"
		exit 1
	fi

	"$(BIN)" seed -id seed -manifest "$(MANIFEST)" -file "$(TEST_FILE)" \
		-tracker "$(TRACKER_URL)" -up-limit "$(UP_LIMIT)" >"$(DEMO_DIR)/cs-seed.log" 2>&1 &
	SEED_PID=$$!
	PIDS+=("$$SEED_PID")
	sleep 1

	CLIENT_PIDS=()
	for i in $$(seq 1 "$(PEERS)"); do
		"$(BIN)" get -id "cs-$$i" -manifest "$(MANIFEST)" \
			-out "$(DEMO_DIR)/cs-$$i.bin" -tracker "$(TRACKER_URL)" \
			-mode cs -result "$(DEMO_DIR)/cs-$$i.json" \
			>"$(DEMO_DIR)/cs-$$i.log" 2>&1 &
		pid=$$!
		CLIENT_PIDS+=("$$pid")
		PIDS+=("$$pid")
	done

	echo "    Aguardando $(PEERS) clientes concluírem o download da semente..."
	for pid in "$${CLIENT_PIDS[@]}"; do
		wait "$$pid"
	done

	expected_hash=$$(sha256sum "$(TEST_FILE)" | awk '{print $$1}')
	for i in $$(seq 1 "$(PEERS)"); do
		got_hash=$$(sha256sum "$(DEMO_DIR)/cs-$$i.bin" | awk '{print $$1}')
		if [ "$$got_hash" != "$$expected_hash" ]; then
			echo "ERRO: integridade SHA-256 falhou em cs-$$i.bin!"
			exit 1
		fi
	done
	echo "    [OK] Todos os $(PEERS) clientes CS concluíram com SHA-256 idêntico ao original."

demo-p2p: prepare
	@set -euo pipefail
	echo ""
	echo "========================================================================"
	echo " CENÁRIO B: P2P RAREST-FIRST (-mode p2p | $(PEERS) peers | semente $(UP_LIMIT) B/s)"
	echo "========================================================================"
	rm -f "$(DEMO_DIR)"/p2p-*.bin "$(DEMO_DIR)"/p2p-*.json "$(DEMO_DIR)"/p2p-*.log

	PIDS=()
	cleanup() {
		for pid in "$${PIDS[@]:-}"; do
			kill "$$pid" 2>/dev/null || true
		done
		wait 2>/dev/null || true
	}
	trap cleanup EXIT INT TERM

	"$(BIN)" tracker -listen "$(TRACKER_ADDR)" >"$(DEMO_DIR)/p2p-tracker.log" 2>&1 &
	TRACKER_PID=$$!
	PIDS+=("$$TRACKER_PID")

	ready=0
	for _ in $$(seq 1 40); do
		if ! kill -0 "$$TRACKER_PID" 2>/dev/null; then
			echo "ERRO: tracker encerrou inesperadamente:"
			cat "$(DEMO_DIR)/p2p-tracker.log"
			exit 1
		fi
		if command -v curl >/dev/null 2>&1; then
			if curl -sf "$(TRACKER_URL)/swarms" >/dev/null 2>&1; then
				ready=1
				break
			fi
		else
			sleep 0.5
			ready=1
			break
		fi
		sleep 0.1
	done
	if [ "$$ready" -ne 1 ]; then
		echo "ERRO: tracker não respondeu em $(TRACKER_URL)"
		cat "$(DEMO_DIR)/p2p-tracker.log"
		exit 1
	fi

	"$(BIN)" seed -id seed -manifest "$(MANIFEST)" -file "$(TEST_FILE)" \
		-tracker "$(TRACKER_URL)" -up-limit "$(UP_LIMIT)" >"$(DEMO_DIR)/p2p-seed.log" 2>&1 &
	SEED_PID=$$!
	PIDS+=("$$SEED_PID")
	sleep 1

	PEER_PIDS=()
	for i in $$(seq 1 "$(PEERS)"); do
		"$(BIN)" get -id "p2p-$$i" -manifest "$(MANIFEST)" \
			-out "$(DEMO_DIR)/p2p-$$i.bin" -tracker "$(TRACKER_URL)" \
			-mode p2p -picker rarest -up-limit "$(UP_LIMIT)" -linger -result "$(DEMO_DIR)/p2p-$$i.json" \
			>"$(DEMO_DIR)/p2p-$$i.log" 2>&1 &
		pid=$$!
		PEER_PIDS+=("$$pid")
		PIDS+=("$$pid")
	done

	echo "    Aguardando $(PEERS) peers trocarem blocos entre si e concluírem..."
	deadline=$$(( $$(date +%s) + $(TIMEOUT_SEC) ))
	while true; do
		done_cnt=0
		for i in $$(seq 1 "$(PEERS)"); do
			if [ -f "$(DEMO_DIR)/p2p-$$i.json" ]; then
				done_cnt=$$((done_cnt + 1))
			fi
		done
		if [ "$$done_cnt" -ge "$(PEERS)" ]; then
			break
		fi
		if [ "$$(date +%s)" -ge "$$deadline" ]; then
			echo "ERRO: timeout ($(TIMEOUT_SEC)s) aguardando peers P2P concluírem ($$done_cnt/$(PEERS))."
			exit 1
		fi
		sleep 0.25
	done

	for pid in "$${PEER_PIDS[@]}"; do
		kill -TERM "$$pid" 2>/dev/null || true
	done
	for pid in "$${PEER_PIDS[@]}"; do
		wait "$$pid" 2>/dev/null || true
	done

	expected_hash=$$(sha256sum "$(TEST_FILE)" | awk '{print $$1}')
	for i in $$(seq 1 "$(PEERS)"); do
		got_hash=$$(sha256sum "$(DEMO_DIR)/p2p-$$i.bin" | awk '{print $$1}')
		if [ "$$got_hash" != "$$expected_hash" ]; then
			echo "ERRO: integridade SHA-256 falhou em p2p-$$i.bin!"
			exit 1
		fi
	done
	echo "    [OK] Todos os $(PEERS) peers P2P concluíram com SHA-256 idêntico ao original."

report:
	@set -euo pipefail
	echo ""
	echo "========================================================================"
	echo " COMPARATIVO DE EFICIÊNCIA: CLIENTE-SERVIDOR vs P2P"
	echo "========================================================================"
	if command -v python3 >/dev/null 2>&1; then
		sed 's/^|//' <<'PYEOF' | DEMO_DIR="$(DEMO_DIR)" python3 -
	|import glob, json, os
	|
	|demo_dir = os.environ["DEMO_DIR"]
	|
	|def load(prefix):
	|    items = []
	|    for path in sorted(glob.glob(os.path.join(demo_dir, f"{prefix}-*.json"))):
	|        with open(path, encoding="utf-8") as f:
	|            items.append(json.load(f))
	|    return items
	|
	|cs = load("cs")
	|p2p = load("p2p")
	|
	|def show_group(title, items):
	|    print(f"\n--- {title} ---")
	|    print(f"{'Peer':<10} {'Tempo (s)':>10} {'Da semente':>13} {'De outros peers':>17} {'% P2P':>8}")
	|    print("-" * 62)
	|    tot_seed = 0
	|    tot_peers = 0
	|    for it in items:
	|        bf = it.get("bytes_from", {})
	|        from_seed = bf.get("seed", 0)
	|        from_others = sum(v for k, v in bf.items() if k != "seed")
	|        total = from_seed + from_others
	|        pct_p2p = (100.0 * from_others / total) if total > 0 else 0.0
	|        tot_seed += from_seed
	|        tot_peers += from_others
	|        print(f"{it['peer_id']:<10} {it['seconds']:>10.2f} {from_seed/1e6:>10.2f} MB {from_others/1e6:>14.2f} MB {pct_p2p:>7.1f}%")
	|    avg_s = sum(it["seconds"] for it in items) / len(items)
	|    max_s = max(it["seconds"] for it in items)
	|    all_tot = tot_seed + tot_peers
	|    all_pct = (100.0 * tot_peers / all_tot) if all_tot > 0 else 0.0
	|    print("-" * 62)
	|    print(f"Média: {avg_s:.2f}s | Último a terminar (makespan): {max_s:.2f}s | Tráfego entre peers: {all_pct:.1f}%")
	|    return avg_s, max_s
	|
	|cs_avg, cs_max = show_group("CLIENTE-SERVIDOR (-mode cs)", cs)
	|p2p_avg, p2p_max = show_group("P2P RAREST-FIRST (-mode p2p)", p2p)
	|print("")
	|if cs_max > 0 and p2p_max > 0:
	|    print(f">>> GANHO DE EFICIÊNCIA (makespan): P2P foi {cs_max / p2p_max:.2f}x mais rápido ({cs_max:.2f}s -> {p2p_max:.2f}s)")
	|if cs_avg > 0 and p2p_avg > 0:
	|    print(f">>> GANHO DE EFICIÊNCIA (média):    P2P foi {cs_avg / p2p_avg:.2f}x mais rápido ({cs_avg:.2f}s -> {p2p_avg:.2f}s)")
	PYEOF
	elif command -v jq >/dev/null 2>&1; then
		echo "--- CLIENTE-SERVIDOR ---"
		jq -r '"\(.peer_id): \(.seconds | . * 100 | round / 100)s | origem: \(.bytes_from)"' "$(DEMO_DIR)"/cs-*.json
		echo "--- P2P RAREST-FIRST ---"
		jq -r '"\(.peer_id): \(.seconds | . * 100 | round / 100)s | origem: \(.bytes_from)"' "$(DEMO_DIR)"/p2p-*.json
	else
		head -n 50 "$(DEMO_DIR)"/cs-*.json "$(DEMO_DIR)"/p2p-*.json
	fi
	echo ""
	echo "--- PROVA DE INTEGRIDADE (SHA-256) ---"
	sha256sum "$(TEST_FILE)" "$(DEMO_DIR)"/cs-*.bin "$(DEMO_DIR)"/p2p-*.bin

compare: demo-cs demo-p2p report

demo-corrupt: prepare
	@set -euo pipefail
	echo ""
	echo "========================================================================"
	echo " CENÁRIO C: DEFESA CONTRA PEER MALICIOSO (-corrupt 1.0 + banimento)"
	echo "========================================================================"
	rm -f "$(DEMO_DIR)"/corrupt-*

	PIDS=()
	cleanup() {
		for pid in "$${PIDS[@]:-}"; do
			kill "$$pid" 2>/dev/null || true
		done
		wait 2>/dev/null || true
	}
	trap cleanup EXIT INT TERM

	"$(BIN)" tracker -listen "$(TRACKER_ADDR)" >"$(DEMO_DIR)/corrupt-tracker.log" 2>&1 &
	TRACKER_PID=$$!
	PIDS+=("$$TRACKER_PID")

	ready=0
	for _ in $$(seq 1 40); do
		if ! kill -0 "$$TRACKER_PID" 2>/dev/null; then
			echo "ERRO: tracker encerrou inesperadamente:"
			cat "$(DEMO_DIR)/corrupt-tracker.log"
			exit 1
		fi
		if command -v curl >/dev/null 2>&1; then
			if curl -sf "$(TRACKER_URL)/swarms" >/dev/null 2>&1; then
				ready=1
				break
			fi
		else
			sleep 0.5
			ready=1
			break
		fi
		sleep 0.1
	done
	if [ "$$ready" -ne 1 ]; then
		echo "ERRO: tracker não respondeu em $(TRACKER_URL)"
		cat "$(DEMO_DIR)/corrupt-tracker.log"
		exit 1
	fi

	# Semente legítima com limite de banda e peer malicioso sem limite (garante contato imediato e banimento)
	"$(BIN)" seed -id seed -manifest "$(MANIFEST)" -file "$(TEST_FILE)" \
		-tracker "$(TRACKER_URL)" -up-limit "$(UP_LIMIT)" >"$(DEMO_DIR)/corrupt-seed.log" 2>&1 &
	PIDS+=("$$!")

	"$(BIN)" seed -id malicioso -manifest "$(MANIFEST)" -file "$(TEST_FILE)" \
		-tracker "$(TRACKER_URL)" -corrupt 1.0 >"$(DEMO_DIR)/corrupt-bad.log" 2>&1 &
	PIDS+=("$$!")
	sleep 1

	echo "    Baixando arquivo com 1 semente legítima + 1 semente maliciosa (-corrupt 1.0)..."
	"$(BIN)" get -id cliente-ok -manifest "$(MANIFEST)" \
		-out "$(DEMO_DIR)/corrupt-seguro.bin" -tracker "$(TRACKER_URL)" \
		-result "$(DEMO_DIR)/corrupt-cliente.json" \
		>"$(DEMO_DIR)/corrupt-cliente.log" 2>&1

	echo "    Eventos de segurança registrados pelo cliente:"
	(grep -E "falhou no SHA-256|banido|DOWNLOAD COMPLETO" "$(DEMO_DIR)/corrupt-cliente.log" || true) | sed 's/^/      /'

	expected_hash=$$(sha256sum "$(TEST_FILE)" | awk '{print $$1}')
	got_hash=$$(sha256sum "$(DEMO_DIR)/corrupt-seguro.bin" | awk '{print $$1}')
	if [ "$$got_hash" != "$$expected_hash" ]; then
		echo "ERRO: arquivo baixado corrompido!"
		exit 1
	fi
	echo "    [OK] Peer malicioso banido automaticamente e arquivo íntegro (SHA-256: $$got_hash)."

demo-leave: prepare
	@set -euo pipefail
	echo ""
	echo "========================================================================"
	echo " CENÁRIO D: SOBREVIVÊNCIA DO ENXAME APÓS SAÍDA DA SEMENTE (-leave-after $(LEAVE_AFTER))"
	echo "========================================================================"
	rm -f "$(DEMO_DIR)"/leave-*

	PIDS=()
	cleanup() {
		for pid in "$${PIDS[@]:-}"; do
			kill "$$pid" 2>/dev/null || true
		done
		wait 2>/dev/null || true
	}
	trap cleanup EXIT INT TERM

	"$(BIN)" tracker -listen "$(TRACKER_ADDR)" >"$(DEMO_DIR)/leave-tracker.log" 2>&1 &
	TRACKER_PID=$$!
	PIDS+=("$$TRACKER_PID")

	ready=0
	for _ in $$(seq 1 40); do
		if ! kill -0 "$$TRACKER_PID" 2>/dev/null; then
			echo "ERRO: tracker encerrou inesperadamente:"
			cat "$(DEMO_DIR)/leave-tracker.log"
			exit 1
		fi
		if command -v curl >/dev/null 2>&1; then
			if curl -sf "$(TRACKER_URL)/swarms" >/dev/null 2>&1; then
				ready=1
				break
			fi
		else
			sleep 0.5
			ready=1
			break
		fi
		sleep 0.1
	done
	if [ "$$ready" -ne 1 ]; then
		echo "ERRO: tracker não respondeu em $(TRACKER_URL)"
		cat "$(DEMO_DIR)/leave-tracker.log"
		exit 1
	fi

	"$(BIN)" seed -id seed-temporaria -manifest "$(MANIFEST)" -file "$(TEST_FILE)" \
		-tracker "$(TRACKER_URL)" -up-limit "$(UP_LIMIT)" -leave-after "$(LEAVE_AFTER)" \
		>"$(DEMO_DIR)/leave-seed.log" 2>&1 &
	SEED_PID=$$!
	PIDS+=("$$SEED_PID")
	sleep 1

	# Fase 1: peer-1 baixa a primeira cópia (1.0x) da semente e permanece semeando (-linger).
	# Fase 2: peers 2..N entram; a semente atinge -leave-after (1.2x) e SAI DA REDE.
	# Mesmo sem a semente original, o enxame P2P entrega 100% do arquivo para todos os peers.
	PEER_PIDS=()
	echo "    [1/2] peer-1 entrando no enxame enquanto a semente ainda está online..."
	"$(BIN)" get -id "peer-1" -manifest "$(MANIFEST)" \
		-out "$(DEMO_DIR)/leave-1.bin" -tracker "$(TRACKER_URL)" \
		-mode p2p -picker rarest -up-limit "$(UP_LIMIT)" -linger \
		-result "$(DEMO_DIR)/leave-1.json" \
		>"$(DEMO_DIR)/leave-1.log" 2>&1 &
	pid=$$!
	PEER_PIDS+=("$$pid")
	PIDS+=("$$pid")

	deadline=$$(( $$(date +%s) + $(TIMEOUT_SEC) ))
	while [ ! -f "$(DEMO_DIR)/leave-1.json" ]; do
		if [ "$$(date +%s)" -ge "$$deadline" ]; then
			echo "ERRO: timeout aguardando peer-1 concluir a primeira cópia."
			exit 1
		fi
		sleep 0.25
	done

	echo "    [2/2] Peers 2..$(PEERS) entrando; a semente sairá em $(LEAVE_AFTER)x e o enxame completará sem ela..."
	for i in $$(seq 2 "$(PEERS)"); do
		"$(BIN)" get -id "peer-$$i" -manifest "$(MANIFEST)" \
			-out "$(DEMO_DIR)/leave-$$i.bin" -tracker "$(TRACKER_URL)" \
			-mode p2p -picker rarest -up-limit "$(UP_LIMIT)" -linger \
			-result "$(DEMO_DIR)/leave-$$i.json" \
			>"$(DEMO_DIR)/leave-$$i.log" 2>&1 &
		pid=$$!
		PEER_PIDS+=("$$pid")
		PIDS+=("$$pid")
	done

	while true; do
		done_cnt=0
		for i in $$(seq 1 "$(PEERS)"); do
			if [ -f "$(DEMO_DIR)/leave-$$i.json" ]; then
				done_cnt=$$((done_cnt + 1))
			fi
		done
		if [ "$$done_cnt" -ge "$(PEERS)" ]; then
			break
		fi
		if [ "$$(date +%s)" -ge "$$deadline" ]; then
			echo "ERRO: timeout ($(TIMEOUT_SEC)s) aguardando sobrevivência do enxame ($$done_cnt/$(PEERS))."
			echo "--- Log da semente ---"
			cat "$(DEMO_DIR)/leave-seed.log"
			exit 1
		fi
		sleep 0.25
	done

	# Garante que a semente já finalizou (sem travar caso LEAVE_AFTER seja muito alto)
	if kill -0 "$$SEED_PID" 2>/dev/null; then
		kill -TERM "$$SEED_PID" 2>/dev/null || true
	fi
	wait "$$SEED_PID" 2>/dev/null || true

	for pid in "$${PEER_PIDS[@]}"; do
		kill -TERM "$$pid" 2>/dev/null || true
	done
	for pid in "$${PEER_PIDS[@]}"; do
		wait "$$pid" 2>/dev/null || true
	done

	echo "    Log de saída da semente:"
	(grep -E "leave-after|encerrado" "$(DEMO_DIR)/leave-seed.log" || true) | sed 's/^/      /'

	if command -v python3 >/dev/null 2>&1; then
		echo "    Origem dos dados nos peers após a saída da semente:"
		sed 's/^|//' <<'PYEOF' | DEMO_DIR="$(DEMO_DIR)" python3 -
	|import glob, json, os
	|for path in sorted(glob.glob(os.path.join(os.environ["DEMO_DIR"], "leave-*.json"))):
	|    with open(path, encoding="utf-8") as f:
	|        it = json.load(f)
	|    bf = it.get("bytes_from", {})
	|    from_seed = bf.get("seed-temporaria", 0)
	|    from_peers = sum(v for k, v in bf.items() if k != "seed-temporaria")
	|    print(f"      {it['peer_id']:<8} {it['seconds']:>5.2f}s | da semente: {from_seed/1e6:>5.2f} MB | de outros peers: {from_peers/1e6:>5.2f} MB")
	PYEOF
	fi

	expected_hash=$$(sha256sum "$(TEST_FILE)" | awk '{print $$1}')
	for i in $$(seq 1 "$(PEERS)"); do
		got_hash=$$(sha256sum "$(DEMO_DIR)/leave-$$i.bin" | awk '{print $$1}')
		if [ "$$got_hash" != "$$expected_hash" ]; then
			echo "ERRO: integridade SHA-256 falhou em leave-$$i.bin!"
			exit 1
		fi
	done
	echo "    [OK] Semente saiu em $(LEAVE_AFTER)x e todos os $(PEERS) peers completaram 100% com SHA-256 válido."

demo-picker: prepare
	@set -euo pipefail
	echo ""
	echo "========================================================================"
	echo " CENÁRIO E: RAREST-FIRST vs SEQUENCIAL (semente sai após $(PICKER_LEAVE)x | $(PEERS) peers | $(REPS) rodadas)"
	echo "========================================================================"
	rm -f "$(DEMO_DIR)"/picker-*
	
	PIDS=()
	cleanup() {
		for pid in "$${PIDS[@]:-}"; do
			kill "$$pid" 2>/dev/null || true
		done
		wait 2>/dev/null || true
	}
	trap cleanup EXIT INT TERM
	
	for picker in rarest sequential; do
		for rep in $$(seq 1 "$(REPS)"); do
			tag="picker-$$picker-$$rep"
			"$(BIN)" tracker -listen "$(TRACKER_ADDR)" >"$(DEMO_DIR)/$$tag-tracker.log" 2>&1 &
			PIDS+=("$$!")
			for _ in $$(seq 1 40); do
				curl -sf "$(TRACKER_URL)/swarms" >/dev/null 2>&1 && break
				sleep 0.1
			done
	
			"$(BIN)" seed -id seed -manifest "$(MANIFEST)" -file "$(TEST_FILE)" \
				-tracker "$(TRACKER_URL)" -up-limit "$(UP_LIMIT)" -leave-after "$(PICKER_LEAVE)" \
				>"$(DEMO_DIR)/$$tag-seed.log" 2>&1 &
			SEED_PID=$$!
			PIDS+=("$$SEED_PID")
			sleep 1
	
			for i in $$(seq 1 "$(PEERS)"); do
				"$(BIN)" get -id "peer-$$i" -manifest "$(MANIFEST)" \
					-out "$(DEMO_DIR)/$$tag-$$i.bin" -tracker "$(TRACKER_URL)" \
					-mode p2p -picker "$$picker" -up-limit "$(UP_LIMIT)" -linger \
					>"$(DEMO_DIR)/$$tag-$$i.log" 2>&1 &
				PIDS+=("$$!")
			done
	
			# Espera a semente sair e dá PICKER_WAIT segundos para os peers trocarem o que sobrou.
			deadline=$$(( $$(date +%s) + $(TIMEOUT_SEC) ))
			while kill -0 "$$SEED_PID" 2>/dev/null && [ "$$(date +%s)" -lt "$$deadline" ]; do
				sleep 0.25
			done
			sleep "$(PICKER_WAIT)"
			cleanup
			PIDS=()
	
			# Cada peer grava o bitfield final ao encerrar; a união diz quanto do arquivo sobrou no enxame.
			sed 's/^|//' <<-'PYEOF' | DEMO_DIR="$(DEMO_DIR)" TAG="$$tag" python3 -
			|import glob, os, re
			|d, tag = os.environ["DEMO_DIR"], os.environ["TAG"]
			|pat = re.compile(r"bitfield final \((\d+)/(\d+) blocos\): ([0-9a-f]+)")
			|union, total, complete, peers = 0, 1, 0, 0
			|for log in sorted(glob.glob(os.path.join(d, tag + "-[0-9]*.log"))):
			|    found = pat.findall(open(log, encoding="utf-8").read())
			|    if not found:
			|        continue
			|    have, total, bits = found[-1]
			|    total = int(total)
			|    union |= int(bits, 16)
			|    complete += int(have) == total
			|    peers += 1
			|frac = 100.0 * bin(union).count("1") / total
			|picker, rep = tag.split("-")[1:]
			|print(f"    {picker:<10} rodada {rep}: {frac:5.1f}% do arquivo ainda no enxame | {complete}/{peers} peers completos")
			|with open(os.path.join(d, "picker-resultados.csv"), "a", encoding="utf-8") as f:
			|    f.write(f"{picker},{rep},{frac:.1f},{complete},{peers}\n")
			PYEOF
		done
	done
	
	echo "    Resumo (média, mín-máx da fração do arquivo que sobreviveu à saída da semente):"
	sed 's/^|//' <<'PYEOF' | DEMO_DIR="$(DEMO_DIR)" python3 -
	|import os
	|rows = {}
	|for line in open(os.path.join(os.environ["DEMO_DIR"], "picker-resultados.csv"), encoding="utf-8"):
	|    picker, _, frac, _, _ = line.strip().split(",")
	|    rows.setdefault(picker, []).append(float(frac))
	|for picker, v in rows.items():
	|    print(f"      {picker:<10} {sum(v) / len(v):5.1f}%  ({min(v):.1f}-{max(v):.1f}%, {len(v)} rodadas)")
	PYEOF

demo: compare demo-corrupt demo-leave
	@echo ""
	echo "========================================================================"
	echo " DEMONSTRAÇÃO COMPLETA FINALIZADA COM SUCESSO!"
	echo " Logs e JSONs salvos em: $(DEMO_DIR)/"
	echo "========================================================================"

swarms:
	@set -euo pipefail
	if command -v jq >/dev/null 2>&1; then
		curl -s "$(TRACKER_URL)/swarms" | jq
	else
		curl -s "$(TRACKER_URL)/swarms"
		echo ""
	fi

clean:
	@set -euo pipefail
	rm -rf "$(DEMO_DIR)" "$(BIN)"
	echo "Limpeza concluída: $(DEMO_DIR)/ e $(BIN) removidos."
