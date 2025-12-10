from app.core.config import settings
import json
import psycopg2
from datetime import datetime
import os


class VideoDatabase:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Подключение к PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=settings.DB_HOST,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                port=settings.DB_PORT,
            )
            self.cursor = self.connection.cursor()
            print("Подключение к базе данных установлено")
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            raise

    def create_tables(self):
        """Создание таблиц"""
        create_videos_table = """
        CREATE TABLE IF NOT EXISTS videos (
            id UUID PRIMARY KEY,
            creator_id VARCHAR(255),
            video_created_at TIMESTAMP WITH TIME ZONE,
            views_count INTEGER,
            likes_count INTEGER,
            comments_count INTEGER,
            reports_count INTEGER,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
        """
        
        create_snapshots_table = """
        CREATE TABLE IF NOT EXISTS video_snapshots (
            id UUID PRIMARY KEY,
            video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
            views_count INTEGER,
            likes_count INTEGER,
            comments_count INTEGER,
            reports_count INTEGER,
            delta_views_count INTEGER,
            delta_likes_count INTEGER,
            delta_comments_count INTEGER,
            delta_reports_count INTEGER,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
        
        -- Индексы для ускорения запросов
        CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
        CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id);
        CREATE INDEX IF NOT EXISTS idx_videos_created_at ON videos(video_created_at);
        """
        
        try:
            self.cursor.execute(create_videos_table)
            self.cursor.execute(create_snapshots_table)
            self.connection.commit()
            print("Таблицы созданы успешно")
        except Exception as e:
            print(f"Ошибка создания таблиц: {e}")
            self.connection.rollback()
            raise

    def load_videos_data(self, json_file_path):
        """Загрузка данных из JSON файла"""
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                videos = data.get('videos', [])
                
                print(f"Найдено {len(videos)} видео для загрузки")
                
                # Загрузка данных в таблицу videos
                videos_inserted = 0
                for video in videos:
                    try:
                        self.cursor.execute("""
                            INSERT INTO videos 
                            (id, creator_id, video_created_at, views_count, likes_count, 
                             comments_count, reports_count, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                creator_id = EXCLUDED.creator_id,
                                video_created_at = EXCLUDED.video_created_at,
                                views_count = EXCLUDED.views_count,
                                likes_count = EXCLUDED.likes_count,
                                comments_count = EXCLUDED.comments_count,
                                reports_count = EXCLUDED.reports_count,
                                updated_at = EXCLUDED.updated_at
                        """, (
                            video['id'],
                            video['creator_id'],
                            video['video_created_at'],
                            video['views_count'],
                            video['likes_count'],
                            video['comments_count'],
                            video['reports_count'],
                            video['created_at'],
                            video['updated_at']
                        ))
                        videos_inserted += 1
                        
                        # Загрузка снапшотов
                        snapshots = video.get('snapshots', [])
                        for snapshot in snapshots:
                            self.cursor.execute("""
                                INSERT INTO video_snapshots 
                                (id, video_id, views_count, likes_count, comments_count, reports_count,
                                 delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                                 created_at, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO UPDATE SET
                                    video_id = EXCLUDED.video_id,
                                    views_count = EXCLUDED.views_count,
                                    likes_count = EXCLUDED.likes_count,
                                    comments_count = EXCLUDED.comments_count,
                                    reports_count = EXCLUDED.reports_count,
                                    delta_views_count = EXCLUDED.delta_views_count,
                                    delta_likes_count = EXCLUDED.delta_likes_count,
                                    delta_comments_count = EXCLUDED.delta_comments_count,
                                    delta_reports_count = EXCLUDED.delta_reports_count,
                                    updated_at = EXCLUDED.updated_at
                            """, (
                                snapshot['id'],
                                snapshot['video_id'],
                                snapshot['views_count'],
                                snapshot['likes_count'],
                                snapshot['comments_count'],
                                snapshot['reports_count'],
                                snapshot['delta_views_count'],
                                snapshot['delta_likes_count'],
                                snapshot['delta_comments_count'],
                                snapshot['delta_reports_count'],
                                snapshot['created_at'],
                                snapshot['updated_at']
                            ))
                        
                        print(f"  ✓ Видео {video['id'][:8]}... загружено ({len(snapshots)} снапшотов)")
                        
                    except Exception as e:
                        print(f"  ✗ Ошибка загрузки видео {video.get('id', 'unknown')}: {e}")
                        continue
                
                self.connection.commit()
                print(f"Загрузка завершена. Загружено: {videos_inserted} видео")
                
        except FileNotFoundError:
            print(f"Файл {json_file_path} не найден")
        except json.JSONDecodeError as e:
            print(f"Ошибка парсинга JSON: {e}")
        except Exception as e:
            print(f"Ошибка загрузки данных: {e}")
            self.connection.rollback()

    def get_statistics(self):
        """Получение статистики по загруженным данным"""
        try:
            # Количество видео
            self.cursor.execute("SELECT COUNT(*) FROM videos")
            video_count = self.cursor.fetchone()[0]
            
            # Количество снапшотов
            self.cursor.execute("SELECT COUNT(*) FROM video_snapshots")
            snapshot_count = self.cursor.fetchone()[0]
            
            # Первые 5 видео для проверки
            self.cursor.execute("""
                SELECT id, creator_id, views_count, likes_count, 
                       video_created_at::date as upload_date
                FROM videos 
                ORDER BY video_created_at DESC 
                LIMIT 5
            """)
            sample_videos = self.cursor.fetchall()
            
            print(f"\n Статистика базы данных:")
            print(f"   Всего видео: {video_count}")
            print(f"   Всего снапшотов: {snapshot_count}")
            print(f"\nПример видео:")
            for video in sample_videos:
                print(f"   • {video[0][:8]}... - создатель: {video[1][:8]}..., просмотры: {video[2]}, лайки: {video[3]}, дата: {video[4]}")
            
        except Exception as e:
            print(f"Ошибка получения статистики: {e}")

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Соединение с базой данных закрыто")

def main():
    """Основная функция"""
    db = VideoDatabase()
    
    try:
        # 1. Подключение
        db.connect()
        
        # 2. Создание таблиц
        db.create_tables()
        
        # 3. Загрузка данных из JSON
        json_file = "/opt/data/videos.json"  # Укажите путь к вашему файлу
        db.load_videos_data(json_file)
        
        # 4. Показать статистику
        db.get_statistics()
        
    except Exception as e:
        print(f"Основная ошибка: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()