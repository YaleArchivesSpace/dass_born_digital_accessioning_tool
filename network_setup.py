#/usr/bin/python3

import json
import os
import yaml

def disconnect():
	os.system("/opt/cisco/anyconnect/bin/vpn disconnect")

def start_connection():
	disconnect()
	with open('config.yml') as config_file:
		cfg = yaml.safe_load(config_file.read())
		vpn_credential_path = cfg.get("vpn_credential_path")
		vpn_command_text = f"/opt/cisco/anyconnect/bin/vpn  -s < {vpn_credential_path}"
		os.system(vpn_command_text)
		print('Connected to VPN, mounting drive')
		# this should work to connect to network drive. But have to be connected to VPN first.
		network_drive_mount_path = cfg.get("network_drive_mount_path")
		mount_command_text = f"osascript -e 'mount volume \"smb://{network_drive_mount_path}\"'"
		os.system(mount_command_text)
		print('Drive mounted')

def main():
	start_connection()


if __name__ == "__main__":
	main()
