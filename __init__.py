import logging
from dotenv import load_dotenv
load_dotenv()

logging.getLogger(__name__).addHandler(logging.NullHandler())

from .risk_data_job import RiskDataJobManager
from .client import Client
from .edm import EDMManager
from .portfolio import PortfolioManager
from .mri_import import MRIImportManager
from .analysis import AnalysisManager
from .treaty import TreatyManager
from .reference_data import ReferenceDataManager
from .rdm import RDMManager
from .import_job import ImportJobManager

class IRPClient:
    """Main client for IRP integration providing access to all managers"""
    
    def __init__(self):
        self._client = Client()
        self.risk_data_job = RiskDataJobManager(self._client)
        self.edm = EDMManager(self._client)
        self.portfolio = PortfolioManager(self._client)
        self.mri_import = MRIImportManager(self._client)
        self.analysis = AnalysisManager(self._client)
        self.treaty = TreatyManager(self._client)
        self.reference_data = ReferenceDataManager(self._client)
        self.rdm = RDMManager(self._client)
        self.import_job = ImportJobManager(self._client)

    @property
    def client(self):
        """Get the underlying API client"""
        return self._client

__all__ = ['IRPClient']