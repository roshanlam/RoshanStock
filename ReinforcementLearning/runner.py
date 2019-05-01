import sys, os, time, configparser
from environment import Simulator
from agent import PolicyGradientAgent, CriticsAgent
import datetime
import numpy as np

cur_path = os.path.dirname(os.path.abspath(__file__))
for _ in range(2):
    root_path = cur_path[0:cur_path.rfind('/', 0, len(cur_path))]
    cur_path = root_path
sys.path.append(root_path + "/" + 'Source/DataBase/')



def rf_run(start_date, end_date):
    actions = ["buy", "sell", "hold"]
    
    training_date = (end_date - datetime.timedelta(days=1 * 365))
    n_iter = 5
    for i in range(n_iter):
        env_train = Simulator(['AMD', 'NVDA'], start_date, training_date)
        agent = PolicyGradientAgent(lookback=env_train.init_state())
       
        action = agent.init_query()

        while env_train.has_more():
            action = actions[action] 
            
            reward, state = env_train.step(action)
            action = agent.query(state, reward)

    env_test = Simulator(['AMD', 'NVDA'], training_date, end_date)
    agent.reset(lookback=env_test.init_state())
    while env_test.has_more():
        action = actions[action] 
      
        reward, state = env_test.step(action)
        action = agent.query(state, reward)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(root_path + "/" + "config.ini")
    storeType = int(config.get('Setting', 'StoreType'))

    now_date = datetime.datetime.now().date()
    start_date = (now_date - datetime.timedelta(days=10 * 365))#.strftime("%Y-%m-%d")
    end_date = now_date#.strftime("%Y-%m-%d")
    rf_run(start_date, end_date)

   
