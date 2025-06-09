# Release Workflows

Este proyecto tiene tres tipos de workflows para generar releases:

## 1. Release Automático (`auto-release.yml`)

Se ejecuta automáticamente cuando:
- Se hace push a la rama `main`
- La versión en `pyproject.toml` cambia
- No se modifican archivos de documentación

**Funcionamiento:**
1. Detecta cambios en la versión del código
2. **Ejecuta CI completo** (lint, format, type check, tests)
3. **Solo si el CI pasa:** crea un tag git con la nueva versión
4. Genera el release en GitHub con changelog automático
5. Crea archivos descargables (.whl y .tar.gz)

## 2. Release Manual (`release.yml`)

Se puede ejecutar de dos formas:

### a) Por Tag Git
```bash
git tag v1.2.3
git push origin v1.2.3
```

### b) Manualmente desde GitHub
1. Ve a Actions → Manual Release → Run workflow
2. Introduce la versión (ej: `1.2.3`)
3. Marca si es prerelease (opcional)
4. Click en "Run workflow"

## 3. Release por Comando (`release-command.yml`)

Se ejecuta desde comentarios en Pull Requests:

### Comandos disponibles:

```bash
# Release patch (1.0.0 → 1.0.1)
/release patch

# Release minor (1.0.0 → 1.1.0)
/release minor

# Release major (1.0.0 → 2.0.0)
/release major

# Release versión específica
/release 1.5.0

# Release prerelease
/release patch --prerelease
```

### Funcionamiento:
1. Comenta en un PR con el comando `/release`
2. El bot calcula la nueva versión
3. Responde con el plan de release
4. Cuando el PR se mergea, actualiza la versión y hace commit
5. **El auto-release detecta el cambio y ejecuta CI + release**

## Permisos Necesarios

- **Auto-release**: Se ejecuta automáticamente
- **Manual release**: Cualquier usuario con permisos de escritura
- **Release command**: Solo usuarios con permisos `admin` o `write`

## Configuración (Paquete Interno)

Este es un paquete interno de Teamcore que se distribuye vía GitHub releases, no PyPI.

Los workflows automáticamente:
- Crean releases en GitHub con archivos descargables
- Generan tags para instalación con pip desde GitHub
- **No publican en PyPI** (esto está deshabilitado para paquetes internos)

## Formato de Versiones

- **Producción**: `1.0.0`, `1.2.3`
- **Prerelease**: `1.0.0-alpha.1`, `1.0.0-beta.2`, `1.0.0-rc.1`

## Changelog Automático

Los releases incluyen automáticamente:
- Lista de commits desde el último release
- Instrucciones de instalación
- Archivos del paquete (wheel y tarball)

## Troubleshooting

### El release automático no se ejecuta
- Verifica que la versión en `pyproject.toml` haya cambiado
- Asegúrate de que no solo se modificaron archivos de documentación

### Instalación desde GitHub
```bash
# Instalar última versión
pip install git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git

# Instalar versión específica
pip install git+https://github.com/ENG-Teamcore4-0/tc-nats-events.git@v1.0.0
```

### El comando `/release` no funciona
- Verifica que tengas permisos de escritura en el repositorio
- Asegúrate de comentar en un Pull Request, no en un Issue