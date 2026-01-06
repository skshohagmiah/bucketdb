#!/bin/bash
# scripts/gen_certs.sh
# Generates self-signed certificates for BucketDB development

set -e

mkdir -p certs
cd certs

echo "🔐 Generating CA..."
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 -out ca.crt -subj "/CN=BucketDB-CA"

# Generate certs for nodes
nodes=("localhost" "node-1" "node-2" "node-3")

for node in "${nodes[@]}"; do
    echo "🔐 Generating cert for $node..."
    openssl genrsa -out ${node}.key 2048
    openssl req -new -key ${node}.key -out ${node}.csr -subj "/CN=${node}"
    
    # Add SAN for localhost and the node name
    cat > ${node}.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${node}
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF

    openssl x509 -req -in ${node}.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out ${node}.crt -days 365 -sha256 -extfile ${node}.ext
done

# Cleanup
rm *.csr *.ext *.srl
echo "✅ Certificates generated in scripts/certs/"
