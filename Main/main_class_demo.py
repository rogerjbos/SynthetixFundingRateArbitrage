from GlobalUtils.logger import *
from pubsub import pub
from APICaller.master.MasterCaller import MasterCaller
from MatchingEngine.MatchingEngine import matchingEngine
from MatchingEngine.profitabilityChecks.checkProfitability import ProfitabilityChecker
from PositionMonitor.Master.MasterPositionMonitorUtils import *
from GlobalUtils.globalUtils import *
from GlobalUtils.marketDirectory import MarketDirectory
import time
import json

import pandas as pd
pd.set_option('display.max_columns', None)

import sys
sys.path.append('/Users/rogerbos/R_HOME/clickhouse_utils/clickhouse_utils')
from clickhouse_client import ClickhouseClient

# connect to clickhouse
ch_client = ClickhouseClient()

class Demo:
    def __init__(self):
        setup_topics()
        self.caller = MasterCaller()
        self.matching_engine = matchingEngine()
        self.profitability_checker = ProfitabilityChecker()
        MarketDirectory.initialize()
    
    def search_for_opportunities(self):
        
        # helper function to calculate profit_loss for dataframe
        def pnl(row, time_to_neutralize: str | float, exchange_funding_rate: float, exchange: str, size_per_exchange = 30) -> float:            
            if time_to_neutralize == "No Neutralization":
                return x.default_trade_size_usd * float(exchange_funding_rate)
            else:
                profit_loss = x.estimate_profit_for_exchange(time_to_neutralize, size_per_exchange, row, exchange)
                return profit_loss if profit_loss is not None else 0

        try:
            funding_rates = self.caller.get_funding_rates()
            opportunities = self.matching_engine.find_delta_neutral_arbitrage_opportunities(funding_rates)
            opportunities = self.profitability_checker.find_most_profitable_opportunity(opportunities, is_demo=True)

            # NEW
            opportunities_df = pd.DataFrame(opportunities)  
            x = ProfitabilityChecker()
            size = x.default_trade_size_usd
            
            # Use lambda functions to apply profitability metrics to the dataframe
            opportunities_df['long_time_to_neutralize'] = opportunities_df.apply(
                lambda row: x.estimate_time_to_neutralize_funding_rate_for_exchange(row, size, row['long_exchange']), axis=1)
            opportunities_df['short_time_to_neutralize'] = opportunities_df.apply(
                lambda row: x.estimate_time_to_neutralize_funding_rate_for_exchange(row, size, row['short_exchange']), axis=1)
            
            opportunities_df['long_profit_loss'] = opportunities_df.apply(
                lambda row: pnl(row, row['long_time_to_neutralize'], row['long_exchange_funding_rate'], row['long_exchange']), axis=1)
            opportunities_df['short_profit_loss'] = opportunities_df.apply(
                lambda row: pnl(row, row['short_time_to_neutralize'], row['short_exchange_funding_rate'], row['short_exchange']), axis=1)

            opportunities_df['projected_profit_usd'] = opportunities_df['total_profit_usd'] + opportunities_df['long_profit_loss'] + opportunities_df['short_profit_loss']
            opportunities_df['size'] = size
            opportunities_df['timestamp'] = time.time()
            opportunities_df.replace("No Neutralization", 0, inplace=True)
            # print(opportunities_df)
            ch_client.save(opportunities_df, "synthetix.opportunities", primary_keys="block_number, long_exchange, short_exchange, symbol", append=True)
                      
            #     opportunity[f'{role}_exchange_profit_loss'] = profit_loss
            #     total_profit_usd += profit_loss
            # 
            # 
            # with open('DEMO_opportunity_visualisations.json', 'w') as file:
            #     json.dump(opportunities, file, indent=4)

        except Exception as e:
            logger.error(f"MainClass - An error occurred during search_for_opportunities: {e}", exc_info=True)
            
    def start_search(self):
        while True:
            self.search_for_opportunities()
            time.sleep(60) 
            
