from typing import Dict, List, Optional, Tuple

import numpy as np
from pydantic import BaseModel, root_validator


class CustomBaseModel(BaseModel):

    class Config:
        """Model configuration."""

        allow_population_by_field_name = True


class Coordinate2D(BaseModel):
    """Container with 2D point coordinates.

    Args:
        r: float
        altitude: float

    """
    r: float
    altitude: float


class RayCoordinate(Coordinate2D):
    """Container with ray coordinate.

    Args:
        time: ray time in current point

    """
    time: float


class Coordinate3D(BaseModel):
    """Container with coordinate data.

    Args:
        x: float
        y: float
        altitude: float

    """
    x: float
    y: float
    altitude: float

    def __eq__(self, other: 'Coordinate3D') -> bool:
        """Compares Coordinate3D objects for equality.

        Args:
            other: Coordinate3D object to compare

        Returns:
            True if both Coordinate3D objects are equal, otherwise - False
        """
        return self.x == other.x and self.y == other.y and (
            self.altitude == other.altitude
        )

    def __ne__(self, other: 'Coordinate3D') -> bool:
        """Compares Coordinate3D objects for inequality.

        Args:
            other: Coordinate3D object to compare

        Returns:
            True if both Coordinate3D objects aren`t equal, otherwise - False
        """
        return self.x != other.x or self.y != other.y or (
            self.altitude != other.altitude
        )

    def get_lateral_distance(self, other: 'Coordinate3D') -> float:
        """Return lateral distance between two points.

        Args:
            other: Coordinate3D

        Returns: float

        """
        return ((self.x - other.x) ** 2 + (self.y - other.y) ** 2) ** 0.5

    def transform_to_2d(self, origin: 'Coordinate3D') -> Coordinate2D:
        """Transform from Coordinate3D to Coordinate2D.

        Args:
            origin: base 3D point

        Returns: Coordinate2D

        """
        return Coordinate2D(
            r=self.get_lateral_distance(other=origin),
            altitude=self.altitude
        )

    def format_to_list(self) -> List[float]:
        """Return list format.

        Returns: List[x, y, altitude]

        """
        return [self.x, self.y, self.altitude]


class Station(BaseModel):
    """Container with station info.

    Args:
         number: station number
         coordinate: Coordinate

    """
    number: int
    coordinate: Coordinate3D

    def __eq__(self, other: 'Station') -> bool:
        """Compares Station objects for equality.

        Args:
            other: Station object to compare

        Returns:
            True if both Station objects are equal, otherwise - False
        """
        return (
            self.number == other.number and self.coordinate == other.coordinate
        )

    def __ne__(self, other: 'Station') -> bool:
        """Compares Station objects for inequality.

        Args:
            other: Station object to compare

        Returns:
            True if both Station objects aren`t equal, otherwise - False
        """
        return (
            self.number != other.number or self.coordinate != other.coordinate
        )

    def format_to_list(self) -> List[float]:
        return [self.number] + self.coordinate.format_to_list()


class Range(BaseModel):
    min_: float
    max_: float

    def __eq__(self, other: 'Range') -> bool:
        """Compares Range objects for equality.

        Args:
            other: Range object to compare

        Returns:
            True if both Range objects are equal, otherwise - False
        """
        return self.min_ == other.min_ and self.max_ == other.max_

    def __ne__(self, other: 'Range') -> bool:
        """Compares Range objects for inequality.

        Args:
            other: Range object to compare

        Returns:
            True if both Range objects aren`t equal, otherwise - False
        """
        return self.min_ != other.min_ or self.max_ != other.max_

    @property
    def size(self) -> float:
        """Return range size.

        Returns: difference between max anf min values

        """
        return self.max_ - self.min_

    @property
    def middle(self) -> float:
        """Return middle value of range.

        Returns: float

        """
        return (self.min_ + self.max_) / 2

    def get_intersection(self, other: 'Range') -> Optional['Range']:
        """Return range after intersection.

        Args:
            other: Range object

        Returns: Range object

        """
        left_edge = max(self.min_, other.min_)
        right_edge = min(self.max_, other.max_)
        if left_edge >= right_edge:
            return None
        return Range(min_=left_edge, max_=right_edge)

    def is_full_include(self, other: 'Range') -> bool:
        if other.size > self.size:
            return False

        intersection = self.get_intersection(other=other)
        if intersection is None:
            return False

        return intersection.size == other.size

    def is_value_belong(self, value: float) -> bool:
        """Check is value in range interval.

        Args:
            value: float

        Returns: bool

        """
        return self.min_ <= value < self.max_

    def format_to_list(self) -> List[float]:
        """Return list format.

        Returns: List[min, max]

        """
        return [self.min_, self.max_]

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        if values['min_'] > values['max_']:
            raise ValueError('Invalid range parameters')
        return values


class Layer(BaseModel):
    """Container with layer description.

    Args:
        altitude_range: Range
        vp: velocity of P-wave in layer

    """
    altitude_range: Range
    vp: float

    def __eq__(self, other: 'Layer') -> bool:
        """Compares Layer objects for equality.

        Args:
            other: Layer object to compare

        Returns:
            True if both Layer objects are equal, otherwise - False
        """
        return (
            self.altitude_range == other.altitude_range and self.vp == other.vp
        )

    def __ne__(self, other: 'Layer') -> bool:
        """Compares Layer objects for inequality.

        Args:
            other: Layer object to compare

        Returns:
            True if both Layer objects aren`t equal, otherwise - False
        """
        return (
            self.altitude_range != other.altitude_range or self.vp != other.vp
        )

    @property
    def thickness(self) -> float:
        """Return layer thickness.

        Returns: float

        """
        return self.altitude_range.size

    @property
    def p_wave_time(self) -> float:
        """Return p-wave time arrival in layer.

        Returns: float

        """
        return self.thickness / self.vp

    @property
    def vs(self) -> float:
        """Return default velocity of S-wave.

        Returns: float

        """
        return self.vp / 2

    @property
    def density(self) -> float:
        """Return default layer density in [g/cm^3].

        See https://en.wikipedia.org/wiki/Gardner%27s_relation

        Returns: float

        """
        return 0.31 * self.vp ** 0.25

    def format_to_dict(self) -> Dict[str, float]:
        """Return layer in dict format.

        Returns: dict

        """
        return {
            'top': int(self.altitude_range.max_),
            'bottom': int(self.altitude_range.min_),
            'v_p': round(self.vp, 1),
            'v_s': round(self.vs, 1),
            'density': round(self.density, 2)
        }

    def format_to_list(self) -> List[float]:
        return self.altitude_range.format_to_list() + [
            self.vp, self.vs, self.density
        ]

    def is_altitude_belong(self, altitude: float) -> bool:
        """Check is altitude belong layer depth range.

        Args:
            altitude: float

        Returns: bool

        """
        return self.altitude_range.is_value_belong(value=altitude)

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        if values['vp'] <= 0:
            raise ValueError('Invalid velocity')
        return values


class SeismicModel(BaseModel):
    """Container with seismic model description."""

    layers: List[Layer]

    def __eq__(self, other: 'SeismicModel') -> bool:
        """Compares SeismicModel objects for equality.

        Args:
            other: SeismicModel object to compare

        Returns:
            True if both SeismicModel objects are equal, otherwise - False
        """
        return self.layers == other.layers

    def __ne__(self, other: 'SeismicModel') -> bool:
        """Compares SeismicModel objects for inequality.

        Args:
            other: SeismicModel object to compare

        Returns:
            True if both SeismicModel objects aren`t equal, otherwise - False
        """
        return self.layers != other.layers

    @property
    def layers_count(self) -> int:
        """Return layers count in model.

        Returns: int

        """
        return len(self.layers)

    @property
    def altitude_range(self) -> Range:
        """Return altitude range of model.

        Returns: Range

        """
        return Range(
            min_=self.layers[-1].altitude_range.min_,
            max_=self.layers[0].altitude_range.max_
        )

    def get_avg_velocity(self, altitude_range: Range) -> float:
        """Return average velocity in altitude range.

        Args:
            altitude_range: Range

        Returns: float

        """
        if altitude_range.max_ > self.altitude_range.max_:
            raise ValueError('Invalid top altitude value')
        if altitude_range.min_ < self.altitude_range.min_:
            raise ValueError('Invalid bottom altitude value')
        if altitude_range.size == 0:
            return self.get_layer_by_altitude(altitude=altitude_range.max_).vp

        total_thickness, total_wave_time = 0, 0
        for layer in self.layers:
            alt_interval = layer.altitude_range
            intersection = alt_interval.get_intersection(other=altitude_range)
            if intersection is None:
                continue

            total_thickness += intersection.size
            total_wave_time += intersection.size / layer.vp
        return total_thickness / total_wave_time

    def get_layer_index_by_altitude(self, altitude: float) -> int:
        """Return model layer index by altitude value.

        Args:
            altitude: float

        Returns: int

        """
        if altitude > self.altitude_range.max_:
            raise ValueError('Altitude is more then max altitude of model')
        if altitude < self.altitude_range.min_:
            raise ValueError('Altitude is less then min altitude of model')
        for i, layer in enumerate(self.layers):
            if layer.is_altitude_belong(altitude=altitude):
                return i

    def get_layer_by_altitude(self, altitude: float) -> Layer:
        """Return layer by altitude value.

        Args:
            altitude: float

        Returns: Layer

        """
        return self.layers[self.get_layer_index_by_altitude(altitude=altitude)]

    def format_to_list(self) -> List[List[float]]:
        """Return model in list format.

        Returns: List[List[float]]

        """
        layers = []
        for layer in self.layers:
            layers.append(layer.altitude_range.format_to_list() + [layer.vp])
        return layers

    def convert_to_numpy_format(self) -> np.ndarray:
        numpy_arr = np.zeros(
            shape=(self.layers_count, 3),
            dtype=np.float32
        )
        for i in range(self.layers_count):
            numpy_arr[i] = self.layers[i].format_to_list()[:3]
        return numpy_arr

    @staticmethod
    def create_from_numpy_array(arr: np.ndarray) -> 'SeismicModel':
        if len(arr.shape) != 2 or arr.shape[1] != 3:
            raise ValueError('Invalid array size')

        layers = []
        for row in arr:
            bottom_altitude, top_altitude, vp = row
            layers.append(
                Layer(
                    altitude_range=Range(
                        min_=float(bottom_altitude),
                        max_=float(top_altitude)
                    ),
                    vp=float(vp)
                )
            )
        return SeismicModel(layers=layers)

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        if not values['layers']:
            raise ValueError('No layers in model')

        sorted_layers = sorted(
            values['layers'],
            key=lambda x: x.altitude_range.max_,
            reverse=True
        )
        for i in range(len(sorted_layers) - 1):
            top_layer, bottom_layer = sorted_layers[i: i + 2]
            bottom_edge = bottom_layer.altitude_range.max_
            top_edge = top_layer.altitude_range.min_
            if bottom_edge != top_edge:
                raise ValueError('Invalid seismic model')

        values['layers'] = sorted_layers
        return values


class LateralExtent(BaseModel):
    """Container with lateral extent properties.

    Args:
        x: Range (extent by X coordinate)
        y: Range (extent by Y coordinate)

    """

    x: Range
    y: Range

    def is_point_belong(self, point: Coordinate3D) -> bool:
        """Return is point belong current extent.

        Args:
            point: Coordinate3D

        Returns: bool

        """
        is_x_belong = self.x.is_value_belong(value=point.x)
        is_y_belong = self.y.is_value_belong(value=point.y)

        return is_x_belong and is_y_belong


class ObservationSystem(BaseModel):
    """Container with observation system."""
    stations: List[Station]

    def __eq__(self, other: 'ObservationSystem') -> bool:
        """Compares ObservationSystem objects for equality.

        Args:
            other: ObservationSystem object to compare

        Returns:
            True if both ObservationSystem objects are equal, otherwise - False
        """
        return self.stations == other.stations

    def __ne__(self, other: 'ObservationSystem') -> bool:
        """Compares ObservationSystem objects for inequality.

        Args:
            other: ObservationSystem object to compare

        Returns:
            True if both ObservationSystem objects aren`t equal,
            otherwise - False
        """
        return self.stations != other.stations

    @property
    def origin(self) -> Coordinate3D:
        """Return minimal coordinate of extent.

        Returns: Coordinate3D

        """
        x_origin = min(x.coordinate.x for x in self.stations)
        y_origin = min(x.coordinate.y for x in self.stations)
        z_origin = min(x.coordinate.altitude for x in self.stations)
        return Coordinate3D(x=x_origin, y=y_origin, altitude=z_origin)

    @property
    def stations_count(self) -> int:
        """Return stations count.

        Returns: int

        """
        return len(self.stations)

    @property
    def minimal_altitude(self) -> float:
        """Return minimal altitude of stations.

        Returns: float

        """
        return min(x.coordinate.altitude for x in self.stations)

    @property
    def maximal_altitude(self) -> float:
        """Return maximal altitude of stations.

        Returns: float

        """
        return max(x.coordinate.altitude for x in self.stations)

    @property
    def relief_difference(self) -> float:
        """Return relief difference.

        Returns: float

        """
        return self.maximal_altitude - self.minimal_altitude

    @property
    def station_numbers(self) -> List[int]:
        """Return station names list.

        Returns: List[int]

        """
        return [x.number for x in self.stations]

    @property
    def lateral_extent(self) -> LateralExtent:
        """Return lateral extent of observation system.

        Returns: LateralExtent

        """
        x_min = min(x.coordinate.x for x in self.stations)
        x_max = max(x.coordinate.x for x in self.stations)

        y_min = min(x.coordinate.y for x in self.stations)
        y_max = max(x.coordinate.y for x in self.stations)
        return LateralExtent(
            x=Range(min_=x_min, max_=x_max),
            y=Range(min_=y_min, max_=y_max)
        )

    def get_station_by_number(self, number: int) -> Station:
        """Return station by name.

        Args:
            number: int

        Returns: Station

        """
        for station in self.stations:
            if station.number == number:
                return station
        raise KeyError('Station not found')

    def get_station_index_by_number(self, number: int) -> int:
        for i, station in enumerate(self.stations):
            if station.number == number:
                return i
        raise KeyError('Station not found')

    def convert_to_numpy_format(self) -> np.ndarray:
        numpy_arr = np.zeros(
            shape=(self.stations_count, 4),
            dtype=np.float32
        )
        for i in range(self.stations_count):
            numpy_arr[i] = self.stations[i].format_to_list()
        return numpy_arr

    @staticmethod
    def create_from_numpy_array(arr: np.ndarray) -> 'ObservationSystem':
        if len(arr.shape) != 2 or arr.shape[1] != 4:
            raise ValueError('Invalid array size')

        stations = []
        for row in arr:
            number, x, y, altitude = row
            stations.append(
                Station(
                    number=int(number),
                    coordinate=Coordinate3D(
                        x=float(x),
                        y=float(y),
                        altitude=float(altitude)
                    )
                )
            )
        return ObservationSystem(stations=stations)

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        station_numbers = [x.number for x in values['stations']]
        if len(set(station_numbers)) != len(station_numbers):
            raise KeyError('Not unique station names')
        return values


class Stepping(BaseModel):
    """Container with stepping values.

    Args:
        dx: step of x axes
        dy: step of y axes
        dz: step of z (altitude) axes

    """
    dx: float
    dy: float
    dz: float

    def __eq__(self, other: 'Stepping') -> bool:
        """Compares Stepping objects for equality.

        Args:
            other: Stepping object to compare

        Returns:
            True if both Stepping objects are equal, otherwise - False
        """
        return (
            self.dx == other.dx and self.dy == other.dy and self.dz == other.dz
        )

    def __ne__(self, other: 'Stepping') -> bool:
        """Compares Stepping objects for inequality.

        Args:
            other: Stepping object to compare

        Returns:
            True if both Stepping objects aren`t equal, otherwise - False
        """
        return (
            self.dx != other.dx or self.dy != other.dy or self.dz != other.dz
        )

    @property
    def max_step(self) -> float:
        """Return max value of steps.

        Returns: float

        """
        return max(self.dx, self.dy, self.dz)

    def reduce(self, factor: int = 2) -> 'Stepping':
        """Return half of stepping.

        Returns: Stepping

        """
        factor = max(factor, 2)
        return Stepping(
            dx=self.dx / factor,
            dy=self.dy / factor,
            dz=self.dz / factor
        )

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        """Check input value."""
        if values['dx'] <= 0:
            raise ValueError('Dx must be more than zero')
        if values['dy'] <= 0:
            raise ValueError('Dy must be more than zero')
        if values['dz'] <= 0:
            raise ValueError('Dz must be more than zero')
        return values


class Spacing(BaseModel):
    """Container with spacing values.

    Args:
        nx: nodes count on x axes
        ny: nodes count on axes
        nz: nodes count on z (altitude) axes

    """
    nx: int
    ny: int
    nz: int

    def __eq__(self, other: 'Spacing') -> bool:
        """Compares Spacing objects for equality.

        Args:
            other: Spacing object to compare

        Returns:
            True if both Spacing objects are equal, otherwise - False
        """
        return (
            self.nx == other.nx and self.ny == other.ny and self.nz == other.nz
        )

    def __ne__(self, other: 'Spacing') -> bool:
        """Compares Spacing objects for inequality.

        Args:
            other: Spacing object to compare

        Returns:
            True if both Spacing objects aren`t equal, otherwise - False
        """
        return (
            self.nx != other.nx or self.ny != other.ny or self.nz != other.nz
        )

    @property
    def nodes_count(self) -> int:
        """Return nodes count.

        Returns: int

        """
        return self.nx * self.ny * self.nz

    def get_node_id(self, node_id: int) -> Tuple[int, int, int]:
        """Return 3D node id by general_id.

        Args:
            node_id: int

        Returns: Tuple[int, int, int]

        """
        if node_id >= self.nodes_count or node_id < 0:
            raise IndexError('Invalid node id')

        iz = node_id // (self.nx * self.ny)
        iy = (node_id % (self.nx * self.ny)) // self.nx
        ix = (node_id % (self.nx * self.ny)) % self.nx
        return ix, iy, iz

    def format_to_list(self) -> List[int]:
        return [self.nx, self.ny, self.nz]

    @root_validator
    def __check_arguments(cls, values: dict) -> dict:
        if values['nx'] <= 0:
            raise ValueError('Nx must be more than zero')
        if values['ny'] <= 0:
            raise ValueError('Ny must be more than zero')
        if values['nz'] <= 0:
            raise ValueError('Nz must be more than zero')
        return values


class SearchSpace(BaseModel):
    """Container with parameters for search space.

    Args:
        x_range: Range
        y_range: Range
        altitude_range: Range

    """
    x_range: Range
    y_range: Range
    altitude_range: Range

    def is_point_belong(self, point: Coordinate3D) -> bool:
        """Checking point belonging to SearchSpace.

        Args:
            point: Coordinate3D

        Returns: bool

        """
        is_x_belong = self.x_range.is_value_belong(value=point.x)
        is_y_belong = self.y_range.is_value_belong(value=point.y)
        is_z_belong = self.altitude_range.is_value_belong(value=point.altitude)
        return is_x_belong and is_y_belong and is_z_belong

    @property
    def center(self) -> Coordinate3D:
        """Return SearchSpace center.

        Returns: Coordinate3D

        """
        return Coordinate3D(
            x=self.x_range.middle,
            y=self.y_range.middle,
            altitude=self.altitude_range.middle
        )

    def get_stepping(self, spacing: Spacing) -> Stepping:
        dx = round(self.x_range.size / spacing.nx, 1)
        dy = round(self.y_range.size / spacing.ny, 1)
        dz = round(self.altitude_range.size / spacing.nz, 1)
        return Stepping(dx=dx, dy=dy, dz=dz)
