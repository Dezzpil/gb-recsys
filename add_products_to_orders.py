import csv
import random
import sys
import os
from config import config

def add_products_to_purchases(orders_path, products_path, output_path=None):
    if not output_path:
        base_name = os.path.basename(orders_path)
        name, ext = os.path.splitext(base_name)
        output_path = os.path.join(config.ORDERS_DATA_DIR, f"{name}-with-products{ext}")
    
    # Загружаем skuId из файла продуктов в память
    sku_ids = []
    try:
        with open(products_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sku_id = row.get('skuId')
                if sku_id:
                    sku_ids.append(sku_id)
    except FileNotFoundError:
        print(f"Ошибка: Файл продуктов не найден: {products_path}")
        return
    except Exception as e:
        print(f"Ошибка при чтении файла продуктов: {e}")
        return
    
    if not sku_ids:
        print("Ошибка: Список skuId пуст или не удалось прочитать skuId из файла продуктов.")
        return

    # Распределение количества продуктов
    counts = [1, 2, 3]
    weights = [0.6, 0.3, 0.1]
    
    # Итеративно обрабатываем покупки (используя потоки чтения/записи)
    try:
        with open(orders_path, mode='r', encoding='utf-8') as f_in, \
             open(output_path, mode='w', encoding='utf-8', newline='') as f_out:
            
            reader = csv.DictReader(f_in)
            
            # Формируем заголовки для выходного файла
            if not reader.fieldnames:
                print("Ошибка: Файл покупок пуст или не имеет заголовков.")
                return
                
            fieldnames = list(reader.fieldnames) + ['productsSkuIds']
            writer = csv.DictWriter(f_out, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            
            writer.writeheader()
            
            for row in reader:
                # Выбираем количество продуктов
                num_products = random.choices(counts, weights=weights)[0]
                
                # Случайно выбираем skuId (без повторений в одном заказе)
                selected_skus = random.sample(sku_ids, min(num_products, len(sku_ids)))
                
                # Добавляем новую колонку
                row['productsSkuIds'] = ','.join(selected_skus)
                
                writer.writerow(row)
    except FileNotFoundError:
        print(f"Ошибка: Файл покупок не найден: {orders_path}")
        return
    except Exception as e:
        print(f"Ошибка при обработке покупок: {e}")
        return
            
    print(f"Обработка завершена. Результат сохранен в: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python add_products_to_orders.py <purchases_csv> <products_csv> [output_csv]")
    else:
        purch_file = sys.argv[1]
        prod_file = sys.argv[2]
        out_file = sys.argv[3] if len(sys.argv) > 3 else None
        add_products_to_purchases(purch_file, prod_file, out_file)
