from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class FileInfo:
    """Modelo para informações de arquivo"""
    
    name: str
    size: int
    file_type: str
    extension: str
    
    @property
    def size_formatted(self) -> str:
        """Retorna tamanho formatado"""
        if self.size < 1024:
            return f"{self.size} bytes"
        elif self.size < 1024 * 1024:
            return f"{self.size / 1024:.1f} KB"
        else:
            return f"{self.size / (1024 * 1024):.1f} MB"


@dataclass
class ProcessedFileInfo:
    """Modelo para informações de arquivo processado"""
    
    columns_detected: int
    columns_with_types: Dict[str, str]
    total_rows_estimated: int
    format: str
    sheet_used: Optional[str] = None
    
    def get_friendly_type(self, dtype: str) -> str:
        """Converte tipos técnicos para descrições amigáveis"""
        if 'int' in dtype.lower():
            return "Números inteiros"
        elif 'float' in dtype.lower():
            return "Números decimais"
        elif 'object' in dtype.lower():
            return "Texto"
        elif 'datetime' in dtype.lower():
            return "Data/Hora"
        elif 'bool' in dtype.lower():
            return "Verdadeiro/Falso"
        else:
            return str(dtype)


@dataclass
class UploadMetadata:
    """Modelo para metadata de upload"""
    
    original_filename: str
    upload_timestamp: datetime
    schema_used: str
    subject: str
    sub_subject: str
    row_count: int
    column_count: int
    file_size_bytes: int
    original_format: str
    processed_by: str = "data-platform-upload-system"
    
    def to_dict(self) -> Dict[str, str]:
        """Converte para dicionário de strings (para S3 metadata)"""
        return {
            'original_filename': self.original_filename,
            'upload_timestamp': self.upload_timestamp.isoformat(),
            'schema_used': self.schema_used,
            'subject': self.subject,
            'sub_subject': self.sub_subject,
            'row_count': str(self.row_count),
            'column_count': str(self.column_count),
            'file_size_bytes': str(self.file_size_bytes),
            'original_format': self.original_format,
            'processed_by': self.processed_by
        }


@dataclass
class S3FileInfo:
    """Modelo para informações de arquivo no S3"""
    
    file_key: str
    filename: str
    original_filename: str
    upload_date: datetime
    file_size_bytes: int
    schema_used: str
    row_count: int
    column_count: int
    upload_timestamp: str
    
    @property
    def file_size_mb(self) -> float:
        """Retorna tamanho em MB"""
        return round(self.file_size_bytes / (1024 * 1024), 2)
    
    def get_category_icon(self) -> str:
        """Retorna ícone baseado no schema"""
        schema_lower = self.schema_used.lower()
        
        if 'vendas' in schema_lower:
            return "💰"
        elif 'financeiro' in schema_lower:
            return "💳"
        elif 'rh' in schema_lower or 'funcionario' in schema_lower:
            return "👥"
        elif 'sistema' in schema_lower or 'user' in schema_lower:
            return "⚙️"
        else:
            return "📄" 