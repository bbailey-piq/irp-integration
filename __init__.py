from .job import JobManager
from .client import Client
from .edm import EDMManager
from .portfolio import PortfolioManager
from .mri_import import MRIImportManager
from .analysis import AnalysisManager
from .treaty import TreatyManager
from .reference_data import ReferenceDataManager
from .rdm import RDMManager

class IRPClient:
    """Main client for IRP integration providing access to all managers"""
    
    def __init__(self):
        self._client = Client()
        self.edm = EDMManager(self._client)
        self.portfolio = PortfolioManager(self._client)
        self.mri_import = MRIImportManager(self._client)
        self.analysis = AnalysisManager(self._client)
        self.treaty = TreatyManager(self._client)
        self.reference_data = ReferenceDataManager(self._client)
        self.rdm = RDMManager(self.client)
        self.job = JobManager(self.client)

    @property
    def client(self):
        """Get the underlying API client"""
        return self._client

__all__ = ['IRPClient']