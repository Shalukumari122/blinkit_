# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import date

import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from blinkit_.items import Blinkit_roshi, Blinkit_comp


class BlinkitPipeline:
    today_date = str(date.today()).replace('-', '_')

    def __init__(self):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='blinkit_')
            self.cur = self.conn.cursor()

        except Exception as e:
            print(e)
    def process_item(self, item, spider):

        if isinstance(item, Blinkit_roshi):
            try:
                # Create table if it doesn't exist
                self.cur.execute(
                    f"CREATE TABLE IF NOT EXISTS blinkit_roshi_data_table_{self.today_date}(id INT AUTO_INCREMENT PRIMARY KEY,unique_id varchar(255) UNIQUE)"
                )

                # Get existing columns
                self.cur.execute(f"SHOW COLUMNS FROM blinkit_roshi_data_table_{self.today_date}")
                existing_columns = [column[0].lower() for column in self.cur.fetchall()]
                print("Existing columns:", existing_columns)  # Debugging statement

                # Prepare item columns
                item_columns = [
                    column_name.replace(" ", "_").lower() for column_name in item.keys()
                ]
                print("Item columns to add:", item_columns)  # Debugging statement

                # Add new columns if they don't exist
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        try:
                            self.cur.execute(
                                f"ALTER TABLE blinkit_roshi_data_table_{self.today_date} ADD COLUMN `{column_name}` LONGTEXT"
                            )
                            existing_columns.append(column_name)
                            print(f"Added column: {column_name}")  # Debugging statement
                        except Exception as e:
                            print("Error adding column:", e)

            except Exception as e:
                print("Error creating table or adding columns:", e)

            try:
                # Prepare field and value lists for insertion
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field).lower())
                    value_list.append('%s')

                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"INSERT IGNORE INTO blinkit_roshi_data_table_{self.today_date}({fields}) VALUES ({values})"

                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()

                try:
                    # Update `master_table` status
                    if 'unique_id' in item:
                        update_query = "UPDATE blinkit_links_roshi SET status = 'Done' WHERE unique_id = %s"
                        self.cur.execute(update_query, (item['unique_id'],))
                        self.conn.commit()
                    else:
                        print("unique_id not found in item.")
                except Exception as e:
                    print(f"Error updating master_table: {e}")

            except Exception as e:
                print("Error inserting data:", e)



        if isinstance(item, Blinkit_comp):
            try:
                self.cur.execute(
                    f"CREATE TABLE IF NOT EXISTS blinkit_comp_data_table_{self.today_date}(id INT AUTO_INCREMENT PRIMARY KEY,unique_id varchar(255) unique)")
                self.cur.execute(f"SHOW COLUMNS FROM blinkit_comp_data_table_{self.today_date}")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(
                                f"ALTER TABLE blinkit_comp_data_table_{self.today_date} ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into blinkit_comp_data_table_{self.today_date}( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()

                try:
                    # Update `master_table` status
                    if 'unique_id' in item:
                        update_query = "UPDATE blinkit_links_comp SET status = 'Done' WHERE unique_id = %s"
                        self.cur.execute(update_query, (item['unique_id'],))
                        self.conn.commit()
                    else:
                        print("unique_id not found in item.")
                except Exception as e:
                    print(f"Error updating master_table: {e}")
            except Exception as e:
                print(e)



        return item
