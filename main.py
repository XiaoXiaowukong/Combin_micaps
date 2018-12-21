import threading, time
from dealbin import dealbin 
from OOBS import OOBS 
from write2micaps import write2micaps
'''
def p1():
	while True:
		print(1)
		time.sleep(0.5)
		
def p2():
	while True:
		print(2)
		time.sleep(0.5)
'''	
if __name__ == '__main__':
	t1 = threading.Thread(target=dealbin)
	t2 = threading.Thread(target=OOBS)
	t3 = threading.Thread(target=write2micaps)
	
	threads = [t1, t2,t3]
	for thread in threads:
		thread.start()
	for thread in threads:
		thread.join()
			