import ccxt
import os
import time
import threading
import queue
import json
import math
import schedule
import traceback

from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('API_KEY')
secret = os.getenv('SECRET')

def count_sig_digits(precision):
    # Count digits after decimal point if it's a fraction
    if precision < 1:
        return abs(int(round(math.log10(precision))))
    else:
        return 1  # Treat whole numbers like 1, 10, 100 as 1 sig digit
    
def round_to_sig_figs(num, sig_figs):
    if num == 0:
        return 0
    return round(num, sig_figs - int(math.floor(math.log10(abs(num)))) - 1)


def calculateLiquidationTargPrice(_liqprice, _entryprice, _percnt, _round):
    return round_to_sig_figs(_entryprice + (_liqprice - _entryprice) * _percnt, _round)

def reEnterTrade(exchange, symbol, order_side, order_price, order_amount, order_type):
    try:
        # Check if symbol is futures (adjust this check to your actual symbol format)
        if ":USDT" not in symbol:
            print(f"Skipping re-entry order for non-futures symbol: {symbol}")
            return

        # Fetch balance once
        balance_info = exchange.fetch_balance({'type': 'swap'})
        usdt_balance = balance_info.get('USDT', {}).get('free', 0)
        
        estimated_cost = order_amount * order_price
        
        # if usdt_balance < estimated_cost:
        #     print(f"⚠️ Insufficient USDT balance ({usdt_balance}) for order cost ({estimated_cost}). Skipping order.")
        #     return
        
        # First attempt: without posSide (works in one-way mode)
        order = exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=order_side,
            amount=order_amount,
            price=order_price,
            params={
                'reduceOnly': False
            }
        )
        print(f"✅ Re-entry order placed: {order_side} {order_amount} @ {order_price}")
        
    except ccxt.BaseError as e:
        error_msg = str(e)
        # Handle specific phemex error for pilot contract
        if 'Pilot contract is not allowed here' in error_msg:
            print(f"❌ Phemex error: Pilot contract is not allowed for {symbol}. Skipping order.")
            return
        
        # If failed due to position mode, retry with posSide
        if 'TE_ERR_INCONSISTENT_POS_MODE' in error_msg:
            print("🔁 Retrying with (Limit) posSide due to inconsistent position mode...")
            pos_side = 'Long' if order_side == 'buy' else 'Short'
            try:
                order = exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=order_side,
                    amount=order_amount,
                    price=order_price,
                    params={
                        'reduceOnly': False,
                        'posSide': pos_side
                    }
                )
                print(f"✅ Re-entry Limit order (with posSide) placed: {order_side} {order_amount} @ {order_price}")
            except ccxt.BaseError as e2:
                print(f"❌ Re-entry Limit order failed even with posSide: {e2}")
        else:
            print(f"❌ Error placing re-entry Limit order: {e}")

            
def get_position(exchange, symbol):
    positions = exchange.fetch_positions([symbol])
    for p in positions:
        if float(p.get('contracts') or 0) > 0:
            return p
    return None
    
def cancel_orphan_orders(exchange, all_symbols, order_type):
    try:
        positions_map = {}
        try:
            # Fetch positions for all symbols once
            all_positions = exchange.fetch_positions(symbols=all_symbols)
            for p in all_positions:
                symbol = p['symbol']
                contracts = float(p.get('contracts') or p.get('size') or 0)
                side = p.get('side', '').lower()
                positions_map[symbol] = {
                    'has_position': contracts > 0,
                    'side': side
                }
        except Exception as e:
            print("Error fetching positions:", e)
            return

        for symbol in all_symbols:
            try:
                open_orders = exchange.fetch_open_orders(symbol)
                if not open_orders:
                    continue

                position_info = positions_map.get(symbol, {'has_position': False, 'side': None})
                has_position = position_info['has_position']
                current_side = position_info['side']

                for order in open_orders:
                    if order['type'] != order_type:
                        continue

                    order_side = order['side'].lower()  # 'buy' or 'sell'

                    # Cancel all limit orders if no position exists
                    if not has_position:
                        print(f"❌ Cancelling orphaned {order_side.upper()} {order_type} order for {symbol} (no position)")
                        try:
                            exchange.cancel_order(order['id'], symbol)
                        except Exception as e:
                            if "TE_ERR_INCONSISTENT_POS_MODE" in str(e):
                                pos_side_str = "Long" if order_side == "buy" else "Short"
                                print(f"Retrying cancel with posSide={pos_side_str}")
                                exchange.cancel_order(order['id'], symbol, {'posSide': pos_side_str})
                            else:
                                print(f"Error cancelling order: {e}")
                        continue

                    # Cancel limit orders that do not match the position side
                    if (order_side == 'buy' and current_side != 'long') or (order_side == 'sell' and current_side != 'short'):
                        print(f"⚠️ Cancelling mismatched {order_side.upper()} {order_type} order for {symbol} (position side: {current_side})")
                        try:
                            exchange.cancel_order(order['id'], symbol)
                        except Exception as e:
                            if "TE_ERR_INCONSISTENT_POS_MODE" in str(e):
                                pos_side_str = "Long" if order_side == "buy" else "Short"
                                print(f"Retrying cancel with posSide={pos_side_str}")
                                exchange.cancel_order(order['id'], symbol, {'posSide': pos_side_str})
                            else:
                                print(f"Error cancelling order: {e}")

            except Exception as e:
                print(f"Error handling {symbol}: {e}")

    except Exception as e:
        print(f"Global error in cancel_orphan_orders: {e}")

        
def monitor_position_and_reenter(exchange, symbol, position):
    try:
        if position:
            # print(json.dumps(position, indent = 4))
            liquidation_price = float(position.get('liquidationPrice') or 0)
            entry_price = float(position.get('entryPrice') or 0)
            mark_price = float(position.get('markPrice') or 0)
            contracts = float(position.get('contracts') or 0)
            leverage = float(position.get("leverage") or 1)
            notional = float(position.get('notional') or 0)
            price_precision_val = exchange.markets[symbol]['precision']['price']
            price_sig_digits = count_sig_digits(price_precision_val)
            amount_precision_val = exchange.markets[symbol]['precision']['amount']
            amount_sig_digits = count_sig_digits(amount_precision_val)
            side = position.get('side').lower()  # typically 'long' or 'short'
            fromPercnt = 0.2  #20%
            if not liquidation_price or not entry_price or not mark_price:
                return  # Skip if essential data is missing
            # Calculate how far the price has moved toward liquidation.
            if side == 'long':
                closeness = 1 - (abs(mark_price - liquidation_price) / abs(entry_price - liquidation_price))
            else:  # short
                closeness = 1 - (abs(mark_price - liquidation_price) / abs(entry_price - liquidation_price))
            print(f"\n--- {symbol} ---")
            print(f"Side: {side}")
            print(f"Entry Price: {entry_price}")
            print(f"Mark Price: {mark_price}")
            print(f"Liquidation Price: {liquidation_price}")
            print(f"Closeness to Liquidation: {closeness * 100:.2f}%")
            # Fetch all open orders
            open_orders = exchange.fetchOpenOrders(symbol)
            side_str = 'buy' if side == 'long' else 'sell' # smae side
            has_same_side_limit = any(
                o['type'] == 'limit' and o['side'] == side_str for o in open_orders
            )
            if has_same_side_limit:
                print("Same-side limit order already exists. Doing nothing.")
                return
            print("Open Orders: ", open_orders)
            # call on rentry function
            order_side = 'sell' if side == 'short' else 'buy'
            order_price = mark_price
            double_notional = notional * 2
            order_amount = double_notional / mark_price
            order_amount = round_to_sig_figs(order_amount, amount_sig_digits)
            order_type = 'limit'
            triggerPrice = calculateLiquidationTargPrice(entry_price, liquidation_price, fromPercnt, price_sig_digits)
            print("Trigger Price: ", triggerPrice, " and Order Amount: ", order_amount)
            reEnterTrade(exchange, symbol, order_side, triggerPrice, order_amount, order_type)
            # Trigger re-entry logic if close to liquidation
            if closeness >= 0.8:
                print("⚠️  Mark price is 80% close to liquidation! Considering re-entry...")
            else:
                print("✅ Not close enough to liquidation for re-entry.")
        else:
            print(f"No open{symbol} positions found.")
    except ccxt.ExchangeError as e:
        print(f"Exchange error: {e}")
    except KeyError as ke:
        print(f"Missing key: {ke}")
    time.sleep(1)

TRAILING_FOLDER = "trailProfit"
TRAILING_ORDER_FOLDER = "tradeOrder"

# Ensure base folders exist
os.makedirs(os.path.join(TRAILING_FOLDER, "buy"), exist_ok=True)
os.makedirs(os.path.join(TRAILING_FOLDER, "sell"), exist_ok=True)
os.makedirs(TRAILING_ORDER_FOLDER, exist_ok=True)


def safe_filename(symbol):
    return symbol.replace('/', '_').replace(':', '_')

def load_trailing_data(symbol, side):
    filename = f"{safe_filename(symbol)}.json"
    subfolder = 'buy' if side == 'long' else 'sell'
    filepath = os.path.join(TRAILING_FOLDER, subfolder, filename)
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            return json.load(f)
    return None


def save_trailing_data(symbol, data, side):
    filename = f"{safe_filename(symbol)}.json"
    subfolder = 'buy' if side == 'long' else 'sell'
    filepath = os.path.join(TRAILING_FOLDER, subfolder, filename)
    data['side'] = 'buy' if side == 'long' else 'sell'
    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)


def delete_trailing_data(symbol):
    filename = f"{safe_filename(symbol)}.json"
    deleted = False
    for subfolder in ['buy', 'sell']:
        filepath = os.path.join(TRAILING_FOLDER, subfolder, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"🗑️ Deleted trailing data for {symbol} from {subfolder} folder")
            deleted = True
    if not deleted:
        print(f"⚠️ No trailing data found to delete for {symbol}")
    return deleted


def reset_trailing_data(symbol=None):
    if symbol:
        filepath = os.path.join(TRAILING_FOLDER, f"{symbol.replace('/', '_')}.json")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"🧹 Trailing data reset for {symbol}. File deleted.")
        else:
            print(f"🧹 No trailing data file found for {symbol}. Nothing to delete.")
    else:
        for filename in os.listdir(TRAILING_FOLDER):
            filepath = os.path.join(TRAILING_FOLDER, filename)
            os.remove(filepath)
        print("🧹 All trailing data reset. All files deleted.")

# The main trailing stop logic now loads/saves per symbol
def trailing_stop_logic(exchange, position, breath_stop, breath_threshold):
    symbol = position.get('symbol')
    entry_price = float(position.get('entryPrice') or 0)
    mark_price = float(position.get('markPrice') or 0)
    side = position.get('side', '').lower()
    leverage = float(position.get("leverage") or 1)
    contracts = float(position.get('contracts') or 0)

    if not entry_price or not mark_price or side not in ['long', 'short'] or contracts <= 0:
        return


    trailing_data = load_trailing_data(symbol, side) or {
        'threshold': 0.10,
        'profit_target_distance': 0.06
    }

    threshold = trailing_data['threshold']
    profit_target_distance = trailing_data['profit_target_distance']
    order_id = trailing_data.get('orderId')

    change = (mark_price - entry_price) / entry_price if side == 'long' else (entry_price - mark_price) / entry_price
    profit_distance = change * leverage

    print("Leverage: ", leverage)
    unrealized_pnl = (mark_price - entry_price) * contracts if side == 'long' else (entry_price - mark_price) * contracts
    realized_pnl = float(position["info"].get('curTermRealisedPnlRv') or 0)
    addUnreRea = unrealized_pnl + realized_pnl
    # # Round to 4 significant figures inline
    # unrealized_pnl_rounded = round_to_sig_figs(unrealized_pnl, 4)
    # realized_pnl_rounded = round_to_sig_figs(realized_pnl, 4)

    print("\n\nUnrealized PnL:", unrealized_pnl)
    print("Realized PnL:", realized_pnl)
    print(f"Add unrpnl and reapnl: {addUnreRea}")
    print("distance entry - last price (for profit):", profit_distance)
    
    if addUnreRea <= 0.001:
        if order_id:
            try:
                # Try canceling without posSide param first (one-way mode)
                exchange.cancel_order(order_id, symbol=symbol)
                print(f"❌ Canceled previous stop-loss {order_id} without posSide")
            except Exception as e:
                error_msg = str(e)
                # Check if error is related to inconsistent position mode
                if "TE_ERR_INCONSISTENT_POS_MODE" in error_msg:
                    try:
                        # Retry with posSide param (hedge mode)
                        params = {'posSide': 'Long' if side == 'long' else 'Short'}
                        exchange.cancel_order(order_id, symbol=symbol, params=params)
                        print(f"❌ Canceled previous stop-loss {order_id} with posSide param")
                    except Exception as e2:
                        print(f"⚠️ Failed to cancel stop-loss even with posSide: {e2}")
                else:
                    print(f"⚠️ Failed to cancel stop-loss: {e}")
                    
        delete_trailing_data(symbol)
        return

    if profit_distance >= threshold:
        print(f"📈 Hello! {side.capitalize()} position on {symbol} is up {round(change * 100, 2)}%")
        new_stop_price = entry_price * (1 + profit_target_distance / leverage) if side == 'long' else entry_price * (1 - profit_target_distance / leverage)

        if (side == 'long' and new_stop_price <= entry_price) or (side == 'short' and new_stop_price >= entry_price):
            print(f"New stop loss @ {new_stop_price} is not valid relative to entry price @ {entry_price}")
            return

        print(f"🔄 Moving stop-loss to {round(profit_target_distance * 100, 2)}%, at price {new_stop_price:.4f}")

        # ✅ Cancel old order if it exists
        if order_id:
            try:
                # Try canceling without posSide param first (one-way mode)
                exchange.cancel_order(order_id, symbol=symbol)
                print(f"❌ Canceled previous stop-loss {order_id} without posSide")
            except Exception as e:
                error_msg = str(e)
                # Check if error is related to inconsistent position mode
                if "TE_ERR_INCONSISTENT_POS_MODE" in error_msg:
                    try:
                        # Retry with posSide param (hedge mode)
                        params = {'posSide': 'Long' if side == 'long' else 'Short'}
                        exchange.cancel_order(order_id, symbol=symbol, params=params)
                        print(f"❌ Canceled previous stop-loss {order_id} with posSide param")
                    except Exception as e2:
                        print(f"⚠️ Failed to cancel stop-loss even with posSide: {e2}")
                else:
                    print(f"⚠️ Failed to cancel stop-loss: {e}")

        order_created = False

        # ✅ Try creating stop-loss in hedge mode
        try:
            order = exchange.create_order(
                symbol=symbol,
                type='stop',
                side='sell' if side == 'long' else 'buy',
                amount=contracts,
                price=None,
                params={
                    'stopPx': new_stop_price,
                    'triggerType': 'ByLastPrice',
                    'triggerDirection': 1 if side == 'long' else 2,  # 🔥 This line is required
                    'positionIdx': 1 if side == 'long' else 2,
                    'posSide': 'Long' if side == 'long' else 'Short',
                    'closeOnTrigger': True,
                    'reduceOnly': True,
                    'timeInForce': 'GoodTillCancel',
                }
            )
            print(f"✅ Placed new stop-loss at {new_stop_price:.4f} for {symbol}")
            order_created = True
        except Exception as e:
            print(f"⚠️ Hedge mode failed: {e} — retrying in one-way mode")

        # ✅ Fallback: one-way mode
        if not order_created:
            try:
                order = exchange.create_order(
                    symbol=symbol,
                    type='stop',
                    side='sell' if side == 'long' else 'buy',
                    amount=contracts,
                    price=None,
                    params={
                        'stopPx': new_stop_price,
                        'triggerType': 'ByLastPrice',
                        'triggerDirection': 1 if side == 'long' else 2,  # 🔥 This line is required
                        'reduceOnly': True,
                        'closeOnTrigger': True,
                        'timeInForce': 'GoodTillCancel',
                    }
                )
                print(f"✅ Placed stop-loss in one-way mode at {new_stop_price:.4f} for {symbol}")
                order_created = True
            except Exception as e2:
                print(f"❌ Failed again (one-way mode): {e2}")
                return

        # ✅ Save updated trailing data
        if order_created:
            trailing_data['orderId'] = order['id']
            trailing_data['profit_target_distance'] = profit_target_distance + breath_threshold
            trailing_data['threshold'] = threshold + breath_threshold
            trailing_data['order_updated'] = True
            save_trailing_data(symbol, trailing_data, side)

def filename_to_symbol(filename):
    # Example input: "JELLYJELLY_USDT_USDT.json"
    parts = filename.replace(".json", "").split("_")
    if len(parts) < 3:
        return None
    base = parts[0]  # e.g. "JELLYJELLY"
    quote = parts[1]  # e.g. "USDT"
    return f"{base}/{quote}:USDT"


def cleanup_closed_trailing_files(exchange, symbols):
    try:
        positionst = exchange.fetch_positions(symbols=symbols)
    except Exception as e:
        print("❌ Failed to fetch positions for cleanup:", e)
        return

    active = {
        ('buy' if pos.get('side', '').lower() == 'long' else 'sell', f"{safe_filename(pos.get('symbol'))}.json")
        for pos in positionst
        if pos.get('contracts', 0) > 0 and pos.get('side', '').lower() in ['long', 'short']
    }
    
    deleted_symbols = set()

    for subfolder in ['buy', 'sell']:
        path = os.path.join(TRAILING_FOLDER, subfolder)
        try:
            for fname in os.listdir(path):
                if (subfolder, fname) not in active:
                    os.remove(os.path.join(path, fname))
                    print(f"🧹 Deleted stale trailing file: {subfolder}/{fname}")
                    
                    # 🔑 Add symbol to list of deleted ones
                    symbol_name = filename_to_symbol(fname)
                    if symbol_name:
                        deleted_symbols.add(symbol_name)
        except FileNotFoundError:
            continue
        
    # 🔁 Only cancel orphan orders for symbols whose trailing files were deleted
    try:
        if deleted_symbols:
            cancel_orphan_orders(exchange, list(deleted_symbols), 'limit')
    except Exception as e:
        print(f"⚠️ Error while cancelling orphan orders during cleanup: {e}")


cancel_queue = queue.Queue()

def create_exchange():
    return ccxt.phemex({
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': True,
    })

def cancel_thread_func(exchange, pos, symbol, order_type):
    try:
        cancel_orphan_orders(exchange, pos, symbol, order_type)
    except Exception as e:
        print(f"Error in cancel_orphan_orders for {symbol}: {e}")
        traceback.print_exc()

def monitor_thread_func(exchange, symbol, pos):
    try:
        monitor_position_and_reenter(exchange, symbol, pos)
    except Exception as e:
        print(f"Error in monitor_position_and_reenter for {symbol}: {e}")
        traceback.print_exc()

def main_job():
    try:
        # Use the global exchange instance
        global exchange

        markets = exchange.load_markets()
        all_symbols = [symbol for symbol in markets if ":USDT" in symbol]
        positionst = exchange.fetch_positions(symbols=all_symbols)
        usdt_balance = exchange.fetch_balance({'type': 'swap'})['USDT']['free']
        print("USDT Balance: ", usdt_balance)

        for pos in positionst:
            symbol = pos['symbol']
            trailing_stop_logic(exchange, pos, 0.10, 0.10)

            if pos.get('contracts', 0) > 0:
                monitor_position_and_reenter(exchange, symbol, pos)         
                
        # # Run cancel_orphan_orders in its own thread immediately
        # cancel_orphan_orders(exchange, all_symbols, 'limit')

        cleanup_closed_trailing_files(exchange, all_symbols)

    except Exception as e:
        print("Error inside main_job:")
        traceback.print_exc()

if __name__ == "__main__":
    exchange = create_exchange()

    # Schedule main_job every 10 seconds
    schedule.every(10).seconds.do(main_job)

    print("Starting scheduler...")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            print("Scheduler crashed:")
            traceback.print_exc()
            print("Retrying in 10 seconds...")
            time.sleep(10)
