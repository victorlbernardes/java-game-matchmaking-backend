# Proteção da branch main

Este documento descreve as configurações recomendadas para proteger a branch `main` no GitHub.

## Configurações no GitHub (Settings → Branches → main)

### 1. Require pull requests before merging
- [ ] **Require a pull request before merging**
- [ ] **Require approval**: 0 (desenvolvedor solo)
- [ ] **Require conversation resolution before merging**: opcional

### 2. Require status checks to pass before merging
- [ ] **Require status checks to pass before merging**
- [ ] **Require branches to be up to date before merging**
- Checks obrigatórios (nome **exato** reportado pelo job — sem prefixo de workflow):
  - `Validate branch name`
  - `Build and test`

> **Atenção:** o GitHub casa o check obrigatório por **string exata** com o nome reportado
> pelo job. Para um job de workflow o nome é `jobs.<id>.name` e, quando não há `name`, o
> próprio `jobs.<id>`. Portanto `ci / Build and test` (formato de exibição), `ci / build` e
> `build` (id do job) **nunca** são reportados → o PR fica preso em
> "Expected — Waiting for status to be reported". Ao renomear um job, atualize os checks
> obrigatórios na mesma mudança.

### 3. Regras de push
- [ ] **Do not allow bypassing the above settings** (desativar bypass para admins)
- [ ] **Restrict who can push to matching branches**: opcional (desenvolvedor solo)
- [ ] **Allow force pushes**: DESMARCADO
- [ ] **Allow deletions**: DESMARCADO

### 4. Regras de merge
- [ ] **Allow merge commits**: DESMARCADO (manter histórico limpo)
- [ ] **Allow squash merging**: MARCADO (recomendado)
- [ ] **Allow rebase merging**: opcional

## Como aplicar

1. Vá em **Settings → Branches** no repositório
2. Clique em **Add rule** ou **Edit** na regra da `main`
3. Marque as opções acima
4. Salve

## Nota sobre planos do GitHub

- **Branch protection rules** estão disponíveis em todos os planos (incluindo free)
- **Require approval de múltiplos revisores** requer plano Team/Enterprise
- **Restrição de quem pode push** requer plano Team/Enterprise

## Verificação

Após aplicar, um push direto para `main` deve ser bloquido:
```bash
git push origin main
# → remote: error: GH006: Protected branch update failed para refs/heads/main.
```

### Conferir nomes reportados × nomes exigidos

Se um PR ficar em "Expected — Waiting for status to be reported", compare as duas listas:

```bash
TOKEN=<personal access token com leitura de admin>

# nomes que os jobs realmente reportam
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://api.github.com/repos/victorlbernardes/java-game-matchmaking-backend/commits/main/check-runs" \
  | jq -r '.check_runs[].name'

# nomes exigidos pela proteção da main
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://api.github.com/repos/victorlbernardes/java-game-matchmaking-backend/branches/main/protection" \
  | jq -r '.required_status_checks.contexts[]'
```

Os dois conjuntos precisam ser idênticos.
