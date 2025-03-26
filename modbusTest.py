from pymodbus.client import ModbusTcpClient
host = '192.168.1.104'
port = 502 
payload = 9700
register = 40493

client = ModbusTcpClient(host,port)
client.connect()
#client.write_register(41167, payload, slave=2)
result = client.read_holding_registers(register,2,slave=2)
print(result.registers)
client.close()