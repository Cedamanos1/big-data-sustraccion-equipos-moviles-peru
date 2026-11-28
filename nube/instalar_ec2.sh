#!/usr/bin/env bash
# Prepara la EC2 (Amazon Linux 2023): Java 17, Python 3.11, swap, repositorio y dependencias.
# Se ejecuta DENTRO de la instancia (Session Manager):   bash instalar_ec2.sh osiptel-g11-<cuenta>
set -euo pipefail
PREFIJO="${1:?Falta el prefijo de buckets (lo imprime crear_infra.sh)}"
REPO="https://github.com/Cedamanos1/big-data-sustraccion-equipos-moviles-peru.git"

sudo dnf install -y java-17-amazon-corretto-headless python3.11 python3.11-pip git
if ! swapon --show | grep -q swapfile; then               # red de seguridad: la instancia tiene 8 GB
  sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
fi
cd "$HOME"
[ -d proyecto ] || git clone "$REPO" proyecto
cd proyecto && git pull --ff-only || true
python3.11 -m venv .venv && . .venv/bin/activate
pip install --quiet -r requirements.txt

sudo tee /etc/profile.d/osiptel.sh >/dev/null <<ENV
export AWS_REGION=${AWS_REGION:-us-east-2}
export OSIPTEL_S3_BRONZE=${PREFIJO}-bronze
export OSIPTEL_S3_SILVER=${PREFIJO}-silver
export OSIPTEL_S3_GOLD=${PREFIJO}-gold
export SPARK_DRIVER_MEMORY=5g
export SHUFFLE_PARTITIONS=4
ENV
echo "Listo. Abre una sesión nueva (o: source /etc/profile.d/osiptel.sh && source ~/proyecto/.venv/bin/activate)"
echo "Luego:  cd ~/proyecto && python src/nube.py estado && python src/pipeline.py"
