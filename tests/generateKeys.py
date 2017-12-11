import sys
from Crypto.PublicKey import RSA


def main():
	if len(sys.argv) != 3:
		print("Arguments wrong")
		return

	numServers = sys.argv[2]
	keyDir = sys.argv[1]

	for i in range(int(numServers)):
		key = RSA.generate(2048)
		public_filename = "%s/public_keys/%d.pem" % (keyDir, i)
		private_filename = "%s/private_keys/%d.pem"% (keyDir, i)

		f = open(public_filename, 'wb')
		f.write(key.publickey().exportKey('PEM'))
		f.close()
		f = open(private_filename, 'wb')
		f.write(key.exportKey('PEM'))
		f.close()
	client_public_filename = "%s/public_keys/client_key.pem" % (keyDir)
	client_private_filename = "%s/private_keys/client_key.pem" % (keyDir)
	key = RSA.generate(2048)
	f = open(client_public_filename, 'wb')
	f.write(key.publickey().exportKey('PEM'))
	f.close()
	f = open(client_private_filename, 'wb')
	f.write(key.exportKey('PEM'))
	f.close()

if __name__ == '__main__':
	main()