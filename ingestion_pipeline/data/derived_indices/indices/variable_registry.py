from dataclasses import dataclass
from enum import Enum


@dataclass
class Variable:
    short_name: str
    long_name: str
    units: str | None = None


class VarEnum(Enum):
    tas = Variable(
        "tas",
        "Mean temperature",
        "degC",
    )
    tasrange = Variable(
        "tasrange",
        "Temperature range",
        "degC",
    )
    tasskew = Variable("tasskew", "Temperature skewness", "degC")
    pr = Variable("pr", "total_precipitation", "mm day-1")
    tasmin = Variable("tasmin", "Minimum temperature", "degC")
    tasminbaisimip = Variable(
        "tasminbaisimip", "Minimum temperature with bias adjustment", "degC"
    )
    tasmax = Variable(
        "tasmax",
        "Maximum temperature",
        "degC",
    )
    psl = Variable("psl", "Sea level pressure", "Pa")
    sst = Variable("sst", "Sea surface temperature", "degC")
    siconc = Variable("siconc", "Sea ice area fraction", "fraction")
    prsn = Variable("prsn", "Accumulated snowfall precipitation", "mm")
    sfcwind = Variable("sfcwind", "Surface Wind", "m s-1")
    clt = Variable("clt", "Cloud area fraction", "percent")
    stl1 = Variable("stl1", "Soil Temperature level 1", "degC")
    rsds = Variable("rsds", "Surface downwelling shortwave radiation", "W m-2")
    rlds = Variable("rlds", "Surface downwelling longwave radiation", "W m-2")
    rsus = Variable("rsus", "Surface upwelling shortwave radiation", "W m-2")
    rlus = Variable("rlus", "Surface upwelling longwave radiation", "W m-2")
    mrsos = Variable("mrsos", "Moisture in upper portion of soil column", "kg m-2")
    huss = Variable("huss", "Near-surface specific humidity", "kg kg -1")
    hurs = Variable("hurs", "Relative humidity")
    dp = Variable("dp", "Dew point", "degC")
    evspsbl = Variable("evspsbl", "Evaporation", "mm")
    mrro = Variable("mrro", "Total runoff", "kg m-2")
    pethg = Variable("pethg", "Potential Evapotranspiration", "mm")
    prmax_24h = Variable("prmax_24h", "Maximum 24h precipitation", "mm")

    @property
    def short_name(self):
        """
        Get the short name of the variable.

        Returns
        -------
        str
            The short name.
        """
        return self.value.short_name

    @property
    def long_name(self):
        """
        Get the long name of the variable.

        Returns
        -------
        str
            The long name.
        """
        return self.value.long_name

    @property
    def units(self):
        """
        Get the units of the variable.

        Returns
        -------
        str or None
            The units.
        """
        return self.value.units
