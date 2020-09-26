import random
from datetime import datetime, timedelta
import uuid
import os

EVENTS = [
		('Alarm','Ошибка датчика температуры газа на входе', 'tgi'),
		('Alarm','Ошибка датчика температуры газа на выходе', 'tgo'),
		('Alarm','Ошибка датчика температуры воздуха внутри помещения','tai1'),
		('Alarm','Ошибка датчика температуры воздуха внутри помещения КИП', 'tai2'),
		('Alarm','Ошибка датчика температуры наружного воздуха', 'tao'),
		('Alarm','Ошибка датчика давления газа на входе', 'pgi'),
		('Alarm','Ошибка датчика давления газа на выходе', 'pgo'),
		('Alarm','Ошибка датчика перепада давления газа на фильтре 1', 'dg1'),
		('Alarm','Ошибка датчика перепада давления газа на фильтре 2', 'dg2'),
		('Alarm','Температура газа на входе ниже нормы', 'tgi'),
		('Alarm','Температура газа на входе выше нормы','tgi'),
		('Alarm','Температура газа на выходе ниже нормы','tgo'),
		('Alarm','Температура газа на выходе выше нормы','tgo'),
		('Alarm','Температура воздуха внутри помещения ниже нормы','tai1'),
		('Alarm','Температура воздуха внутри помещения выше нормы','tai1'),
		('Alarm','Температура воздуха внутри помещения КП ниже нормы','tai2'),
		('Alarm','Температура воздуха внутри помещения КП выше нормы','tai2'),
		('Alarm','Давление газа на входе ниже нормы', 'pgi'),
		('Alarm','Давление газа на входе выше нормы','pgi'),
		('Alarm','Давление газа на выходе ниже нормы','pgo'),
		('Alarm','Давление газа на выходе выше нормы','pgo'),
		('Alarm','Перепад давления газа на фильтре 1 ниже нормы', 'dg1'),
		('Alarm','Перепад давления газа на фильтре 1 выше нормы','dg1'),
		('Alarm','Перепад давления газа на фильтре 2 ниже нормы','dg2'),
		('Alarm','Перепад давления газа на фильтре 2 выше нормы','dg2'),
		('Alarm','Пожар в технологическом помещении ГРПБ', 'falr1'),
		('Alarm','Охрана ГРПБ', 'balr1'),
		('Alarm','Пожар в помещении КИП', 'falr2'),
		('Alarm','Охрана КИП', 'balr2'),
		('Alarm','Загазованность CH4', 'CH4'),
		('Event','Питание от ввода 1 380В', 'mns1'),
		('Event','Питание от ввода 2 380В', 'mns2'),
		('Alarm','Отсутствие питания 380В', 'mns1'),
		('Event','Задвижка на входе ГРПБ открыта', 'auma1'),
		('Event','Задвижка на входе ГРПБ открывается', 'auma1'),
		('Event','Задвижка на входе ГРПБ закрыта', 'auma1'),
		('Event','Задвижка на входе ГРПБ закрывается', 'auma1'),
		('Event','Удаленное управление задвижкой на входе ГРПБ', 'auma1'),
		('Event','Локальное управление задвижкой на входе ГРПБ', 'auma1'),
		('Event','Задвижка на входе ГРПБ не готова к управлению', 'auma1'),
		('Warning','Общее предупреждение задвижка на входе ГРПБ', 'auma1'),
		('Alarm','Ошибка связи задвижки на входе ГРПБ', 'auma1'),
		('Alarm','Общая авария задвижки на входе ГРПБ', 'auma1'),
		('Alarm','Ошибка муфты на открытие задвижки на входе ГРПБ', 'auma1'),
		('Alarm','Ошибка муфты на закрытие задвижки на входе ГРПБ', 'auma1'),
		('Alarm','Авария фазы задвижки на входе ГРПБ', 'auma1'),
		('Alarm','Авария температуры задвижки на входе ГРПБ', 'auma1'),
		('Action','Команда открыть задвижку на входе ГРПБ', 'auma1'),
		('Action','Команда закрыть задвижку на входе ГРПБ', 'auma1'),
		('Action','Команда остановить задвижку на входе ГРПБ', 'auma1'),
		('Event','Задвижка на выходе ГРПБ открыта', 'auma2'),
		('Event','Задвижка на выходе ГРПБ открывается', 'auma2'),
		('Event','Задвижка на выходе ГРПБ закрыта', 'auma2'),
		('Event','Задвижка на выходе ГРПБ закрывается', 'auma2'),
		('Event','Удаленное управление задвижкой на выходе ГРПБ', 'auma2'),
		('Event','Локальное управление задвижкой на выходе ГРПБ', 'auma2'),
		('Event','Задвижка на выходе ГРПБ не готова к управлению', 'auma2'),
		('Warning','Общее предупреждение задвижка на выходе ГРПБ', 'auma2'),
		('Alarm','Ошибка связи задвижки на выходе ГРПБ', 'auma2'),
		('Alarm','Общая авария задвижки на выходе ГРПБ', 'auma2'),
		('Alarm','Ошибка муфты на открытие задвижки на выходе ГРПБ', 'auma2'),
		('Alarm','Ошибка муфты на закрытие задвижки на выходе ГРПБ', 'auma2'),
		('Alarm','Авария фазы задвижки на выходе ГРПБ', 'auma2'),
		('Alarm','Авария температуры задвижки на выходе ГРПБ', 'auma2'),
		('Action','Команда открыть задвижку на выходе ГРПБ', 'auma2'),
		('Action','Команда закрыть задвижку на выходе ГРПБ', 'auma2'),
		('Action','Команда остановить задвижку на выходе ГРПБ', 'auma2'),
		('Event','Задвижка на входе в литейный цех открыта', 'auma3'),
		('Event','Задвижка на входе в литейный цех открывается', 'auma3'),
		('Event','Задвижка на входе в литейный цех закрыта', 'auma3'),
		('Event','Задвижка на входе в литейный цех закрывается', 'auma3'),
		('Event','Удаленное управление задвижкой на входе в литейный цех', 'auma3'),
		('Event','Локальное управление задвижкой на входе в литейный цех', 'auma3'),
		('Event','Задвижка на входе в литейный цех не готова к управлению', 'auma3'),
		('Warning','Общее предупреждение задвижка на входе в литейный цех', 'auma3'),
		('Alarm','Ошибка связи задвижки на входе в литейный цех', 'auma3'),
		('Alarm','Общая авария задвижки на входе в литейный цех', 'auma3'),
		('Alarm','Ошибка муфты на открытие задвижки на входе в литейный цех', 'auma3'),
		('Alarm','Ошибка муфты на закрытие задвижки на входе в литейный цех', 'auma3'),
		('Alarm','Авария фазы задвижки на входе в литейный цех', 'auma3'),
		('Alarm','Авария температуры задвижки на входе в литейный цех', 'auma3'),
		('Action','Команда открыть задвижку на входе в литейный цех', 'auma3'),
		('Action','Команда закрыть задвижку на входе в литейный цех', 'auma3'),
		('Action','Команда остановить задвижку на входе в литейный цех', 'auma3'),
		('Event','Задвижка секционная на ГРПБ открыта', 'auma4'),
		('Event','Задвижка секционная на ГРПБ открывается', 'auma4'),
		('Event','Задвижка секционная на ГРПБ закрыта', 'auma4'),
		('Event','Задвижка секционная на ГРПБ закрывается', 'auma4'),
		('Event','Удаленное управление задвижкой секционной на ГРПБ', 'auma4'),
		('Event','Локальное управление задвижкой секционной на ГРПБ', 'auma4'),
		('Event','Задвижка секционная на ГРПБ не готова к управлению', 'auma4'),
		('Warning','Общее предупреждение секционной на ГРПБ', 'grb'),
		('Alarm','Ошибка связи задвижки секционной на ГРПБ', 'auma4'),
		('Alarm','Общая авария секционной задвижки на ГРПБ', 'auma4'),
		('Alarm','Ошибка муфты на открытие задвижки секционной на ГРПБ', 'auma4'),
		('Alarm','Ошибка муфты на закрытие задвижки секционной на ГРПБ', 'auma4'),
		('Alarm','Авария фазы задвижки секционной на ГРПБ', 'auma4'),
		('Alarm','Авария температуры задвижки секционной на ГРПБ', 'auma4'),
		('Action','Команда открыть задвижку секционную на ГРПБ', 'auma4'),
		('Action','Команда закрыть задвижку секционную на ГРПБ', 'auma4'),
		('Action','Команда остановить задвижку секционную на ГРПБ','auma4')
	]

SOURCE_NODES = [
					'ГРПБ-У-2-2С',
				 	'ГРПБ-ГМП-У-65R-2В',
				 	'ПУ - 5000'
				]

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

#Описание, датчик, минимальный и максимальный пороги
SENSORS = [
			[
				("Температура газа на входе", "tgi", -10, 30),
				("Температура газа на выходе", "tgo", -10, 30),
				("Температура воздуха в помещении ПУ", "tai1", 0, 40 ),
				("Температура воздуха в помещении КИП", "tai2", 0, 40 ),
				("Температура воздуха на улице", "tao", -50, 40),
				("Давление газа на входе ПУ", "pgi", 0.4, 1),
				("Давление газа на выходе ПУ", "pgo", 0.2, 0.9),
				("Перепад давления на фильтре 1", "dg1", -0.2, 0.2),
				("Перепад давления на фильтре 1", "dg1", -0.2, 0.2)
			],
			[
				("Состояние питающей сети", "mns1", 0, 1),
				("Состояние пожарной сигнализации","falr", 0, 1),
				("Состояние охранной сигнализации","balr", 0, 1),
				("Состояние сигнализатора загазованности", "ch4" ,0, 1)		
			],
			[
				("Параметр задвижки на входе", "auma1", 
					("Открыт", "Открывается", "Закрыт", "Закрывается", "Удаленное управление", "Локальное управление", "Не готов к управлению")),
				("Параметр задвижки на выходе", "auma2", 
					("Открыт", "Открывается", "Закрыт", "Закрывается", "Удаленное управление", "Локальное управление", "Не готов к управлению"))
			],
			[
				("Процент открытости задвижки на входе", "auma1_actual_pos", 0, 100),
				("Процент открытости задвижки на выходе", "auma2_actual_pos", 0, 100)
			]
	
]

class Event(object):

	def __init__(self, sourcenode, eventid, time, event_type, sourcename, message, ack_transitiontime):
		self.sourcenode = sourcenode   #ПУ 5000, ГРГУ
		self.eventid = eventid
		self.time = time 
		self.conditionname = event_type 
		self.message = message
		self.sourcename = sourcename #датчик
		self.ack_transitiontime = ack_transitiontime #время квитирования события
				
	def get_fields(self):
		return f"{self.time},{self.conditionname},{self.message},{self.sourcenode},{self.sourcename},{self.ack_transitiontime}"
 

class Signal(object):

	def __init__(self, nodeid, value, status, sourcetimestamp): 
		self.nodeid= nodeid 
		self.value = value
		self.status = status
		self.sourcetimestamp = sourcetimestamp

	def get_fields(self):
		return f"{self.sourcetimestamp},{self.nodeid},{self.status},{self.value}"


def generate_event(time):
	sourcenode = random.choice(SOURCE_NODES)   #ПУ 5000, ГРГУ
	eventid = uuid.uuid4().int % 1000000
	event_type, message, sourcename = random.choice(EVENTS)
	time = time
	ack_time = time + timedelta(seconds = random.randint(20, 120))
	ack_transitiontime = random.choices(['NULL', ack_time], weights = [5, 2])[0] #время квитирования события


	event = Event(	sourcenode = sourcenode, 
					eventid = eventid,
					time = time.strftime(TIME_FORMAT),
					event_type = event_type,
					sourcename = sourcename,
					message = message,
					ack_transitiontime = ack_transitiontime
				)
	return event


def events_generator(count=1000, begin_date="2020-01-01 00:00:00"):
	time = datetime.strptime(begin_date, TIME_FORMAT)

	os.makedirs('data', exist_ok=True)
	dir_to_save =  os.path.abspath('data')
	file_path = os.path.join(dir_to_save, 'events_log.csv')

	with open(file_path, 'w', encoding='utf8') as logfile:
		for i in range(count):
			time += timedelta(seconds = random.randint(5, 30))
			event = generate_event(time = time)
			logfile.write(event.get_fields())
			logfile.write('\n')


def generate_signal(time, signal_type)->[]:
	signals = []

	if signal_type == "float":
		for s in SENSORS[0]:
			definition, sensor, minimum, maximum = s
			
			nodeid = f"{SOURCE_NODES[0]}/{sensor}"
			value = random.uniform(minimum, maximum)
			status = random.choices([0,1], weights=[10, 1])[0]
			time = time

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[1]}/{sensor}"
			value = random.uniform(minimum, maximum)
		
			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[2]}/{sensor}"
			value = random.uniform(minimum, maximum)
		
			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

	elif signal_type == "int":
		for s in SENSORS[3]:
			definition, sensor, minimum, maximum = s
			
			nodeid = f"{SOURCE_NODES[0]}/{sensor}"
			value = random.randint(minimum, maximum)
			status = random.choices([0,1], weights=[10, 1])[0]
			time = time

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[1]}/{sensor}"
			value = random.randint(minimum, maximum)
		
			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[2]}/{sensor}"
			value = random.randint(minimum, maximum)
		
			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

	elif signal_type == "bool":
		for s in SENSORS[1]:
			definition, sensor, minimum, maximum = s
			
			nodeid = f"{SOURCE_NODES[0]}/{sensor}"
			value = bool(random.randint(minimum, maximum))
			status = 0
			time = time

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[1]}/{sensor}"
			value = bool(random.randint(minimum, maximum))

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[2]}/{sensor}"
			value = bool(random.randint(minimum, maximum))

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

	elif signal_type == "string":
		for s in SENSORS[2]:
			definition, sensor, values = s
			nodeid = f"{SOURCE_NODES[0]}/{sensor}"
			value = random.choice(values)
			status = 0
			time = time

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[1]}/{sensor}"
			value = random.choice(values)

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))

			nodeid = f"{SOURCE_NODES[2]}/{sensor}"
			value = random.choice(values)

			signals.append(Signal(nodeid=nodeid, value=value, status=status, sourcetimestamp=time))
	
	return signals


def signals_generator(count=1000, begin_date="2018-01-01 00:00:00", signal_type="float", interval=30):
	time = datetime.strptime(begin_date, TIME_FORMAT)

	os.makedirs('data', exist_ok=True)
	dir_to_save =  os.path.abspath('data')
	file_path = os.path.join(dir_to_save, signal_type + '_data.csv')
	
	with open(file_path, 'w', encoding='utf8') as logfile:
		while count>0:
			signals = generate_signal(time, signal_type)
			for signal in signals:
				logfile.write(signal.get_fields())
				logfile.write('\n')
			time += timedelta(seconds = interval)
			count -= len(signals)


def main():
# 	print("start")
# 	events_generator(count=20000000)
# 	print("1")
# 	signals_generator(signal_type="float", count=20000000)
# 	print("2")
# 	signals_generator(signal_type="int", count=20000000)
# 	print("3")
# 	signals_generator(signal_type="bool", count=20000000)
# 	print("4")
# 	signals_generator(signal_type="string", count=20000000)
# 	print("done")

#	signals_generator()
	
	events_generator()
	signals_generator(signal_type="float")
	signals_generator(signal_type="int")	
	signals_generator(signal_type="bool")	
	signals_generator(signal_type="string")	


if __name__ == "__main__":
	main()



