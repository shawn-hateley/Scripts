#!/usr/bin/env bash

# Define an array of server IP addresses or hostnames
servers=("10.10.1.25")

# Ask the user for the SSH public key file
read -p "Enter the path to your SSH public key (default: ~/.ssh/id_rsa.pub): " public_key_file
public_key_file=${public_key_file:-"$HOME/.ssh/id_rsa.pub"}

# Check if the public key file exists
if [ ! -f "$public_key_file" ]; then
    echo "Error: Public key file not found: $public_key_file"
    exit 1
fi

# Ask the user for the remote SSH username
read -p "Enter the remote SSH username (default: $USER): " remote_user
remote_user=${remote_user:-$USER}

# Ensure remote_user is not empty
if [[ -z "$remote_user" ]]; then
    echo "Error: Remote username cannot be empty."
    exit 1
fi

# Function to copy public key
copy_key() {
    local server="$1"
    echo "Processing $server..."

    # Check if the server is reachable using SSH
    if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$remote_user@$server" exit &> /dev/null; then
        echo "Error: Server $remote_user@$server is unreachable."
        return 1
    fi

    # Check if the public key is already in the authorized_keys file
    if ssh "$remote_user@$server" "grep -Fxq \"$(<\"$public_key_file\")\" ~/.ssh/authorized_keys" &> /dev/null; then
        echo "Public key already exists on $server for $remote_user."
        return 0
    fi

    # Use ssh-copy-id if available, otherwise fallback to manual method
    if command -v ssh-copy-id &> /dev/null; then
        if ssh-copy-id -i "$public_key_file" "$remote_user@$server"; then
            echo "Public key copied to $server for $remote_user."
        else
            echo "Error: Failed to copy public key to $server for $remote_user."
            return 2
        fi
    else
        echo "Warning: ssh-copy-id not found. Using manual method..."
        if ssh "$remote_user@$server" "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys" < "$public_key_file"; then
            echo "Public key copied to $server for $remote_user."
        else
            echo "Error: Failed to copy public key to $server for $remote_user."
            return 2
        fi
    fi
}

# Loop through each server and copy the public key
for server in "${servers[@]}"; do
    copy_key "$server"
done

echo "Script completed."