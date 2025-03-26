import BAC0
bacnet = BAC0.connect()
VALUE = 20
r = '30100:192.168.1.64:47809 analogValue 800 presentValue ' + str(VALUE)
bacnet.write(r)

