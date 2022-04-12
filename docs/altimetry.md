# Altimetry

Altimetry data (water level, significant wave height and wind speed) obtained from satellites are very useful for validation of models as the data are available globally since 1985 and up to 12 hour before now. 

DHI has an [altimetry portal](https://altimetry.dhigroup.com/purchase) with an [api](https://altimetry-shop-data-api.dhigroup.com/apidoc) where you can download the data. If you have an api key you can access the data through this repo. 

![](https://github.com/DHI/WatObs/raw/main/images/altimetry_overview.png)

```{eval-rst}
.. autoclass:: watobs.DHIAltimetryRepository
	:members:

.. autoclass:: watobs.altimetry.AltimetryData
	:members:
```