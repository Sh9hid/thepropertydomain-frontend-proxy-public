from pathlib import Path
from core.config import STOCK_ROOT
from services.suburb_intel_service import get_suburb_intel


class PropertyIntelligence:
    """
    Property data intelligence using local Cotality xlsx suburb reports.
    DuckDB is used for CSV files; xlsx files use the suburb_intel_service.
    """

    def __init__(self):
        self.stock_path = Path(STOCK_ROOT)

    def analyze_local_market(self) -> dict:
        """Return file count and total xlsx reports available."""
        report_dir = self.stock_path / "Suburb reports"
        if not report_dir.exists():
            return {"status": "Suburb reports directory not found", "files": 0}
        xlsxes = list(report_dir.glob("*.xlsx"))
        return {
            "status": "ok",
            "files": len(xlsxes),
            "suburbs": [f.stem.replace(" report", "") for f in xlsxes],
            "source": "D:\\L+S Stock\\Suburb reports",
        }

    def find_property_trends(self, suburb: str) -> dict:
        """
        Return real suburb stats from Cotality xlsx report.
        Returns from in-memory cache after first read.
        """
        intel = get_suburb_intel(suburb)
        if not intel:
            return {"suburb": suburb, "trend": "No data", "confidence": 0}

        median = intel.get("median_price_recent") or intel.get("median_price")
        recent = intel.get("recent_5y_count", 0)
        total = intel.get("total_records", 0)
        trend = "Increasing" if recent > 0 and total > 0 and recent / max(total, 1) > 0.1 else "Stable"

        return {
            "suburb": suburb,
            "trend": trend,
            "confidence": min(0.95, 0.5 + (recent / max(total, 1))),
            "median_price": median,
            "recent_5y_count": recent,
            "total_records": total,
            "source": intel.get("source_file", "unknown"),
        }


# Singleton instance
intel_engine = PropertyIntelligence()
