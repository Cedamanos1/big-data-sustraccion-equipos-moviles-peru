#!/usr/bin/env bash
# Crea la infraestructura en AWS: 3 buckets S3 (bronze, silver, gold), rol de IAM, grupo de seguridad y la EC2.
# Se ejecuta UNA vez desde AWS CloudShell:   bash nube/crear_infra.sh tu_correo@upao.edu.pe
# Es repetible: lo que ya existe se reutiliza.
set -euo pipefail
CORREO="${1:-}"
REGION="${AWS_REGION:-us-east-2}"; export AWS_DEFAULT_REGION="$REGION"
CUENTA="$(aws sts get-caller-identity --query Account --output text)"
PREFIJO="osiptel-g11-${CUENTA}"
ROL="osiptel-g11-ec2"; PERFIL="osiptel-g11-ec2"; SG="osiptel-g11-sg"; TIPO="m7i-flex.large"
echo "Región: $REGION · cuenta: $CUENTA · prefijo de buckets: $PREFIJO"

echo "== 1/5 Buckets S3 (cifrados, sin acceso público; versionado en bronze) =="
for z in bronze silver gold; do
  B="${PREFIJO}-${z}"
  aws s3api head-bucket --bucket "$B" 2>/dev/null || \
    aws s3api create-bucket --bucket "$B" --region "$REGION" --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
  aws s3api put-public-access-block --bucket "$B" --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-encryption --bucket "$B" --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
  echo "   s3://$B"
done
aws s3api put-bucket-versioning --bucket "${PREFIJO}-bronze" --versioning-configuration Status=Enabled

echo "== 2/5 Rol de IAM para la EC2 (sin claves de acceso) =="
cat > /tmp/confianza.json <<J
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}
J
cat > /tmp/permisos.json <<J
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::${PREFIJO}-bronze","arn:aws:s3:::${PREFIJO}-silver","arn:aws:s3:::${PREFIJO}-gold"]},
 {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],"Resource":["arn:aws:s3:::${PREFIJO}-bronze/*","arn:aws:s3:::${PREFIJO}-silver/*","arn:aws:s3:::${PREFIJO}-gold/*"]},
 {"Effect":"Allow","Action":["athena:*"],"Resource":"*"},
 {"Effect":"Allow","Action":["glue:CreateDatabase","glue:GetDatabase","glue:GetDatabases","glue:CreateTable","glue:UpdateTable","glue:GetTable","glue:GetTables","glue:GetPartition","glue:GetPartitions","glue:BatchCreatePartition","glue:CreatePartition"],"Resource":"*"}
]}
J
aws iam get-role --role-name "$ROL" >/dev/null 2>&1 || \
  aws iam create-role --role-name "$ROL" --assume-role-policy-document file:///tmp/confianza.json >/dev/null
aws iam attach-role-policy --role-name "$ROL" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
aws iam put-role-policy --role-name "$ROL" --policy-name osiptel-g11-datos --policy-document file:///tmp/permisos.json
aws iam get-instance-profile --instance-profile-name "$PERFIL" >/dev/null 2>&1 || {
  aws iam create-instance-profile --instance-profile-name "$PERFIL" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "$PERFIL" --role-name "$ROL"
  echo "   esperando a que el perfil se propague…"; sleep 15; }

echo "== 3/5 Grupo de seguridad (sin puertos de entrada: se entra por Session Manager) =="
VPC="$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --query 'Vpcs[0].VpcId' --output text)"
SGID="$(aws ec2 describe-security-groups --filters Name=group-name,Values=$SG Name=vpc-id,Values=$VPC --query 'SecurityGroups[0].GroupId' --output text)"
if [ "$SGID" = "None" ]; then
  SGID="$(aws ec2 create-security-group --group-name "$SG" --description "OSIPTEL G11 sin entrada" --vpc-id "$VPC" --query GroupId --output text)"
fi
SUBNET="$(aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text)"

echo "== 4/5 EC2 $TIPO (Amazon Linux 2023, 30 GB) =="
AMI="$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)"
ID="$(aws ec2 describe-instances --filters Name=tag:Name,Values=osiptel-g11 Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[0].Instances[0].InstanceId' --output text)"
if [ "$ID" = "None" ]; then
  ID="$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TIPO" --subnet-id "$SUBNET" --security-group-ids "$SGID" \
    --iam-instance-profile Name="$PERFIL" --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3,Encrypted=true}' \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=osiptel-g11}]' --query 'Instances[0].InstanceId' --output text)"
fi
echo "   instancia: $ID"

echo "== 5/5 Alerta de presupuesto (USD 5/mes) =="
if [ -n "$CORREO" ]; then
  aws budgets create-budget --account-id "$CUENTA" \
    --budget '{"BudgetName":"osiptel-g11","BudgetLimit":{"Amount":"5","Unit":"USD"},"TimeUnit":"MONTHLY","BudgetType":"COST"}' \
    --notifications-with-subscribers "[{\"Notification\":{\"NotificationType\":\"ACTUAL\",\"ComparisonOperator\":\"GREATER_THAN\",\"Threshold\":80,\"ThresholdType\":\"PERCENTAGE\"},\"Subscribers\":[{\"SubscriptionType\":\"EMAIL\",\"Address\":\"$CORREO\"}]}]" \
    2>/dev/null && echo "   alerta creada para $CORREO" || echo "   (ya existía o no se pudo crear: créala en Facturación > Budgets)"
else
  echo "   omitida (pasa tu correo como primer argumento para crearla)"
fi

cat <<FIN

LISTO. Guarda estos datos:
  Prefijo de buckets : $PREFIJO      (bronze / silver / gold)
  Instancia EC2      : $ID
Siguiente: sube los 3 archivos originales a s3://${PREFIJO}-bronze/raw/ (robos/, reclamos/, cobertura/) y sigue nube/README_NUBE.md
FIN
