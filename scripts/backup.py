import subprocess
import os
from datetime import datetime
import psycopg2
from pathlib import Path
import shutil
import config
from utils.logger_setup import LoggerSetup

class DatabaseBackup:
    def __init__(self, db_name, db_user, db_password, db_host, backup_dir="backups"):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.backup_dir = backup_dir
        
        # Initialize logger using the shared utility
        logger_setup = LoggerSetup(
            "DatabaseBackup",
            log_dir="logs",
            extra_logger="backup_details"  # For detailed backup information
        )
        self.logger = logger_setup.get_logger()
        self.details_logger = logger_setup.get_extra_logger()

    def create_backup(self, compression_level=9) -> str:
        """Create a backup of the database using pg_dump with enhanced features."""
        try:
            # Create backup directory structure
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_subdir = os.path.join(self.backup_dir, timestamp)
            os.makedirs(backup_subdir, exist_ok=True)
            
            # Generate backup filename
            backup_filename = f"{self.db_name}_backup_{timestamp}.sql"
            backup_path = os.path.join(backup_subdir, backup_filename)
            
            # Set environment variables for pg_dump
            backup_env = os.environ.copy()
            backup_env["PGPASSWORD"] = self.db_password
            
            # Construct pg_dump command with enhanced options
            cmd = [
                "pg_dump",
                "-h", self.db_host,
                "-U", self.db_user,
                "-F", "c",  # Custom format (compressed)
                "-b",  # Include large objects
                "-v",  # Verbose output
                "-Z", str(compression_level),  # Compression level
                "--blobs",  # Include large objects in dump
                "--no-owner",  # Don't include commands to set ownership
                "--no-privileges",  # Don't include privilege settings
                "-f", backup_path,
                self.db_name
            ]
            
            self.logger.info(f"Starting database backup to {backup_path}")
            self.details_logger.info(f"Backup configuration: compression_level={compression_level}")
            
            # Execute pg_dump
            start_time = datetime.now()
            result = subprocess.run(
                cmd,
                env=backup_env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                duration = datetime.now() - start_time
                file_size = os.path.getsize(backup_path)
                
                self.logger.info("Backup completed successfully")
                self.details_logger.info(
                    f"Backup details:\n"
                    f"Duration: {duration}\n"
                    f"Size: {file_size/1024/1024:.2f} MB\n"
                    f"Path: {backup_path}"
                )
                
                # Create backup manifest
                self._create_backup_manifest(backup_subdir, {
                    'timestamp': timestamp,
                    'database': self.db_name,
                    'size': file_size,
                    'duration': str(duration),
                    'compression_level': compression_level
                })
                
                return backup_path
            else:
                self.logger.error(f"Backup failed: {result.stderr}")
                raise Exception(f"Backup failed: {result.stderr}")
                
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
            raise

    def verify_backup(self, backup_path: str) -> bool:
        """Verify the backup file is valid and can be restored."""
        try:
            if not os.path.exists(backup_path):
                self.logger.error(f"Backup file not found: {backup_path}")
                return False
                
            size = os.path.getsize(backup_path)
            if size == 0:
                self.logger.error(f"Backup file is empty: {backup_path}")
                return False
                
            # Test backup file integrity using pg_restore
            verify_cmd = [
                "pg_restore",
                "-l",  # List contents only
                backup_path
            ]
            
            result = subprocess.run(
                verify_cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                self.logger.info(f"Backup verified successfully: {backup_path}")
                self.details_logger.info(f"Verified backup size: {size/1024/1024:.2f} MB")
                return True
            else:
                self.logger.error(f"Backup verification failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error verifying backup: {e}")
            return False

    def _create_backup_manifest(self, backup_dir: str, metadata: dict):
        """Create a manifest file with backup metadata."""
        manifest_path = os.path.join(backup_dir, "backup_manifest.txt")
        try:
            with open(manifest_path, 'w') as f:
                f.write("Database Backup Manifest\n")
                f.write("=======================\n\n")
                for key, value in metadata.items():
                    f.write(f"{key}: {value}\n")
            self.logger.info(f"Created backup manifest: {manifest_path}")
            self.details_logger.info(f"Manifest metadata: {metadata}")
        except Exception as e:
            self.logger.error(f"Error creating backup manifest: {e}")

    def cleanup_old_backups(self, keep_days: int = 30):
        """Remove backup files older than specified days."""
        try:
            cutoff_date = datetime.now().timestamp() - (keep_days * 24 * 60 * 60)
            
            for backup_dir in os.listdir(self.backup_dir):
                backup_path = os.path.join(self.backup_dir, backup_dir)
                if os.path.isdir(backup_path):
                    dir_time = os.path.getctime(backup_path)
                    if dir_time < cutoff_date:
                        shutil.rmtree(backup_path)
                        self.logger.info(f"Removed old backup: {backup_path}")
                        self.details_logger.info(
                            f"Cleanup details:\n"
                            f"Path: {backup_path}\n"
                            f"Age: {(datetime.now().timestamp() - dir_time) / 86400:.1f} days"
                        )
                        
        except Exception as e:
            self.logger.error(f"Error cleaning up old backups: {e}")

def main():
    backup = DatabaseBackup(
        db_name=config.DB_NAME,
        db_user=config.DB_USER,
        db_password=config.DB_PASSWORD,
        db_host=config.DB_HOST
    )
    
    try:
        # Create backup
        backup.logger.info("Starting database backup process")
        backup_path = backup.create_backup(compression_level=9)
        
        # Verify backup
        if backup.verify_backup(backup_path):
            print(f"\nBackup created and verified successfully: {backup_path}")
            
            # Clean up old backups
            if input("Clean up backups older than 30 days? (yes/no): ").lower() == 'yes':
                backup.cleanup_old_backups()
                print("Old backups cleaned up")
        else:
            print("\nBackup verification failed!")
            
    except Exception as e:
        backup.logger.error(f"Fatal error: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()