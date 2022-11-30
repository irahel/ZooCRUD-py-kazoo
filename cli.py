from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, ZookeeperError, NoNodeError, BadVersionError
import sys
from strings import Strings

def Command_treatment(enter):
    splitted = str.split(enter)
    if enter is None or enter == '':
        print(strs.ERROR_PREFIX + strs.MESSAGE_COMMAND_EMPTY)        
        return False
    elif splitted[0] not in reserved_words_commands:
        print(strs.ERROR_PREFIX + strs.MESSAGE_INCORRECT_USAGE_COMMAND)        
        return False
    elif splitted[0] in [reserved_words_commands[0], reserved_words_commands[4]]:
        if len(splitted) > 3:
            print(strs.ERROR_PREFIX + strs.MESSAGE_COMMAND_MANY_ARGUMMENTS)
            print(strs.SERVER_PREFIX + strs.MESSAGE_USAGE_CREATE_COMMAND)
            return False
        elif len(splitted) < 3:
            print(strs.ERROR_PREFIX + strs.MESSAGE_COMMAND_FEW_ARGUMMENTS)
            print(strs.SERVER_PREFIX + strs.MESSAGE_USAGE_CREATE_COMMAND)
            return False
    elif splitted[0] in [
        reserved_words_commands[1],
        reserved_words_commands[2],
        reserved_words_commands[3],
        reserved_words_commands[5],
        reserved_words_commands[6],
    ]:
        if len(splitted) > 2:
            print(strs.ERROR_PREFIX + strs.MESSAGE_COMMAND_MANY_ARGUMMENTS)
            print(strs.SERVER_PREFIX + strs.MESSAGE_USAGE_READ_COMMAND)
            return False
        elif len(splitted) < 2:
            print(strs.ERROR_PREFIX + strs.MESSAGE_COMMAND_FEW_ARGUMMENTS)
            print(strs.SERVER_PREFIX + strs.MESSAGE_USAGE_READ_COMMAND)
            return False
    return True
#C
def Create(path, value):
    try:
        zk.create(path, value.encode())
        print(strs.SERVER_PREFIX + strs.MESSAGE_CREATE_SUCESS)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
        print(strs.SERVER_PREFIX + strs.MESSAGE_CREATE_FAILED)
#R
def Read(path):
    try:
        data, _ = zk.get(path)
        print(strs.SERVER_PREFIX + data.decode("utf-8"))
    except NoNodeError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_EXISTS_FALSE)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
def Exists(path):
    try:
        if zk.exists(path):
            print(strs.SERVER_PREFIX + strs.MESSAGE_EXISTS_TRUE)
        else:
            print(strs.SERVER_PREFIX + strs.MESSAGE_EXISTS_FALSE)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
def List(path):
    try:
        children = zk.get_children(path)
        for item in children:
            print(strs.SERVER_PREFIX + strs.MESSAGE_LIST + item)        
    except NoNodeError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_EXISTS_FALSE)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
#U
def Update(path, value):
    try:
        zk.set(path, value.encode())
        print(strs.SERVER_PREFIX + strs.MESSAGE_UPDATE_SUCESS)
    except BadVersionError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_BAD_VERSION)
        print(strs.SERVER_PREFIX + strs.MESSAGE_UPDATE_FAILED)
    except NoNodeError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_EXISTS_FALSE)
        print(strs.SERVER_PREFIX + strs.MESSAGE_UPDATE_FAILED)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
        print(strs.SERVER_PREFIX + strs.MESSAGE_UPDATE_FAILED)
#D
def Delete(path):
    try:
        zk.delete(path, recursive=False)
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_SUCESS)
    except BadVersionError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_BAD_VERSION)
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_FAILED)
    except NoNodeError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_EXISTS_FALSE)
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_FAILED)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_FAILED)
def Deletec(path):
    try:
        zk.delete(path, recursive=True)
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_SUCESS)
    except BadVersionError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_BAD_VERSION)
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_FAILED)
    except NoNodeError as _:
        print(strs.ERROR_PREFIX + strs.MESSAGE_EXISTS_FALSE)
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_FAILED)
    except ZookeeperError as e:
        print(strs.ERROR_PREFIX + e.__str__())
        print(strs.SERVER_PREFIX + strs.MESSAGE_DELETE_FAILED)

#Helpers
strs = Strings()
reserved_words_commands = ['CREATE', 'READ', 'EXISTS', 'LIST', 'UPDATE', 'DELETE', 'DELETEC', 'QUIT']

#Get data
if len(sys.argv) == 3:
    SERVER_IP = sys.argv[1]
    SERVER_PORT = sys.argv[2]
else:
    print(strs.ERROR_PREFIX + strs.MESSAGE_INCORRECT_USAGE_LINE_COMMAND)
    print(strs.SERVER_PREFIX + strs.MESSAGE_USAGE_LINE_COMMAND)

#Stabilish Connection
try:
    zk = KazooClient(hosts=f'{SERVER_IP}:{SERVER_PORT}')
    zk.start()
    print(strs.SERVER_PREFIX + strs.MESSAGE_CONNECTED + "with " + SERVER_IP+":"+SERVER_PORT)
except KazooException as e:
    print(strs.ERROR_PREFIX + e.__str__())

#Commands
while(True):
    print(strs.CLI_PREFIX, end="")
    command = input()
    if Command_treatment(command):
        argumments = str.split(command)
        if argumments[0] == reserved_words_commands[0]:
            Create(argumments[1], argumments[2])
        elif argumments[0] == reserved_words_commands[1]:
            Read(argumments[1])
        elif argumments[0] == reserved_words_commands[2]:
            Exists(argumments[1])
        elif argumments[0] == reserved_words_commands[3]:
            List(argumments[1])
        elif argumments[0] == reserved_words_commands[4]:
            Update(argumments[1], argumments[2])
        elif argumments[0] == reserved_words_commands[5]:
            Delete(argumments[1])
        elif argumments[0] == reserved_words_commands[6]:
            Deletec(argumments[1])
        elif argumments[0] == reserved_words_commands[7]:
            break

#Finish Connection
zk.stop()
print(strs.CLI_PREFIX + strs.MESSAGE_DISCONNECTED)