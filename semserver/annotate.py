from __future__ import unicode_literals, print_function

import re

import rdflib
from rdflib.namespace import NamespaceManager, XSD
import ztreamy.events
import ztreamy.server
import ztreamy.rdfevents


class GenericAnnotator(object):

    def __init__(self):
        self.ns = {}
        self.uri_ref_cache = {}
        self.namespace_manager = NamespaceManager(rdflib.Graph())
        self.annotation_dispatcher = {}

    def annotate_events(self, events):
        annotated = []
        for event in events:
            annotated.extend(self.annotate_event(event))
        return annotated

    def annotate_event(self, event):
        """Annotates the event and returns a resulting list of events.

        The annotation dispatcher is intended to be configured
        by the subclasses.

        """
        func = self.annotation_dispatcher.get(event.application_id,
                                              self.identity_annotator)
        return func(event)

    def identity_annotator(self, event):
        return [event]

    def register_ns(self, key, uri_prefix, prefix=None):
        self.ns[key] = rdflib.Namespace(uri_prefix)
        if prefix:
            self.namespace_manager.bind(prefix, self.ns[key])

    def _create_graph(self):
        return rdflib.Graph(namespace_manager=self.namespace_manager)

    def _uri_ref(self, key, suffix):
        if (key, suffix) in self.uri_ref_cache:
            uri_ref = self.uri_ref_cache[(key, suffix)]
        elif key in self.ns:
            uri_ref = self.ns[key][suffix]
        else:
            uri_ref = self.ns[''][key + '-' + suffix]
        return uri_ref

    def _create_uri_ref(self, key, suffix):
        uri_ref = self._uri_ref(key, suffix)
        self.uri_ref_cache[(key, suffix)] = uri_ref
        return uri_ref

    def _create_uri_refs(self, data):
        for key, suffix in data:
            self._create_uri_ref(key, suffix)

    def _create_event(self, event, graph):
        return ztreamy.events.Event.create( \
            event.source_id,
            'text/n3',
            graph,
            application_id=event.application_id,
            aggregator_id=event.aggregator_id,
            event_type=event.event_type,
            timestamp=event.timestamp,
            extra_headers={'X-Derived-From': event.event_id})


class HermesAnnotator(GenericAnnotator):

    ns_hermes = 'http://webtlab.it.uc3m.es/ns/hermes#'
    ns_geo = 'http://www.w3.org/2003/01/geo/wgs84_pos#'
    application_id_driver = 'SmartDriver'
    application_id_steps = 'Hermes-Citizen-Fitbit-Steps'
    application_id_heart_rate = 'Hermes-Citizen-Fitbit-HeartRate'
    application_id_sleep = 'Hermes-Citizen-Fitbit-Sleep'

    def __init__(self):
        super(HermesAnnotator, self).__init__()
        self.register_ns('', HermesAnnotator.ns_hermes, prefix='hermes')
        self.register_ns('geo', HermesAnnotator.ns_geo, prefix='geo')
        self._create_uri_refs([
            ('', 'Id-Observation'),
            ('', 'Id-User'),
            ('', 'User'),
            ('', 'Driver'),
            ('', 'Pedestrian'),
            ('', 'Observation'),
            ('', 'Driving'),
            ('', 'Speed'),
            ('', 'Heart_Rate'),
            ('', 'Stopping'),
            ('', 'Agressiveness'),
            ('', 'Acceleration'),
            ('', 'Stepping'),
            ('', 'Step_Set'),
            ('', 'Heart_Frequency'),
            ('', 'Heart_Set'),
            ('', 'Sleep'),
            ('', 'has_user'),
            ('', 'has_driver'),
            ('', 'has_pedestrian'),
            ('', 'orientation'),
            ('', 'has_location'),
            ('', 'completed_distance'),
            ('', 'average_speed'),
            ('', 'deviation_speed'),
            ('', 'inefficient_speed'),
            ('', 'normal_bpm'),
            ('', 'deviation_bpm'),
            ('', 'unexpected_bpm'),
            ('', 'stops'),
            ('', 'positive_kinetic_energy'),
            ('', 'acceleration'),
            ('', 'stepping_date'),
            ('', 'stepping_time'),
            ('', 'steps'),
            ('', 'has_step_set'),
            ('', 'heart_date'),
            ('', 'heart_time'),
            ('', 'bpm'),
            ('', 'has_heart_set'),
            ('', 'awakenings'),
            ('', 'minutes_asleep'),
            ('', 'minutes_in_bed'),
            ('', 'sleeping_date'),
            ('', 'sleeping_start_time'),
            ('', 'sleeping_end_time'),
            ('geo', 'SpatialThing'),
            ('geo', 'lat'),
            ('geo', 'long'),
        ])
        self.annotation_dispatcher.update({
            HermesAnnotator.application_id_driver: self.annotate_event_driver,
            HermesAnnotator.application_id_steps: self.annotate_event_steps,
            HermesAnnotator.application_id_heart_rate: \
                                             self.annotate_event_heart_rate,
            HermesAnnotator.application_id_sleep: self.annotate_event_sleep,
        })
        self.classes_driver = {
            'Average Speed Section': 'Speed',
            'Standard Deviation of Vehicle Speed Section': 'Speed',
            'Inefficient Speed Section': 'Speed',
            'Heart Rate Section': 'Heart_Rate',
            'Standard Deviation Heart Rate Section': 'Heart_Rate',
            'High Heart Rate': 'Heart_Rate',
            'Stops Section': 'Stopping',
            'Positive Kinetic Energy': 'Agressiveness',
            'High Acceleration': 'Acceleration',
            'High Deceleration': 'Acceleration',
        }

    def annotate_event_driver(self, event):
        if (not isinstance(event, ztreamy.events.JSONEvent)
            or len(event.body.keys()) != 1
            or list(event.body.keys())[0] not in self.classes_driver):
            return [event]
        top_key = list(event.body.keys())[0]
        data = event.body[top_key]
        graph = self._create_graph()
        observation = self._uri_ref('Id-Observation', event.event_id)
        graph.add((observation,
                   rdflib.RDF.type,
                   self._uri_ref('', self.classes_driver[top_key])))
        graph.add((observation,
                   self._uri_ref('', 'happens_at_timestamp'),
                   rdflib.Literal(event.timestamp,
                                  datatype=XSD.timestamp)))
        graph.add((observation,
                   self._uri_ref('', 'has_driver'),
                   self._uri_ref('Id-User', event.source_id)))
        graph.add((observation,
                   self._uri_ref('', 'orientation'),
                   rdflib.Literal(data['orientation'])))
        location = rdflib.BNode()
        graph.add((observation,
                   self._uri_ref('', 'has_location'),
                   location))
        graph.add((location,
                   rdflib.RDF.type,
                   self._uri_ref('geo', 'SpatialThing')))
        graph.add((location,
                   self._uri_ref('geo', 'lat'),
                   rdflib.Literal(data['latitude'])))
        graph.add((location,
                   self._uri_ref('geo', 'long'),
                   rdflib.Literal(data['longitude'])))
        if 'distancia' in data:
            graph.add((observation,
                       self._uri_ref('', 'completed_distance'),
                       rdflib.Literal(data['distancia'], normalize=False,
                                      datatype=XSD.double)))
        if top_key == 'Average Speed Section':
            graph.add((observation,
                       self._uri_ref('', 'average_speed'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Standard Deviation of Vehicle Speed Section':
            graph.add((observation,
                       self._uri_ref('', 'deviation_speed'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Inefficient Speed Section':
            graph.add((observation,
                       self._uri_ref('', 'inefficient_speed'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Heart Rate Section':
            graph.add((observation,
                       self._uri_ref('', 'normal_bpm'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'High Heart Rate':
            graph.add((observation,
                       self._uri_ref('', 'unexpected_bpm'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Standard Deviation Heart Rate Section':
            graph.add((observation,
                       self._uri_ref('', 'deviation_bpm'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Stops Section':
            graph.add((observation,
                       self._uri_ref('', 'stops'),
                       rdflib.Literal(int(data['value']))))
        elif top_key == 'Positive Kinetic Energy':
            graph.add((observation,
                       self._uri_ref('', 'positive_kinetic_energy'),
                       rdflib.Literal(data['value'])))
        elif (top_key == 'High Acceleration'
            or top_key == 'High Deceleration'):
            graph.add((observation,
                       self._uri_ref('', 'acceleration'),
                       rdflib.Literal(data['value'])))
        else:
            raise ValueError('Unhandled key: ' + top_key)
        return [self._create_event(event, graph)]

    def annotate_event_steps(self, event):
        if (not isinstance(event, ztreamy.events.JSONEvent)
            or len(event.body.keys()) != 1):
            return [event]
        try:
            dataset = event.body['dataset']
            graph = self._create_graph()
            for i, data in enumerate(dataset):
                observation = self._uri_ref('Id-Observation',
                                            '{}-{}'.format(event.event_id, i))
                graph.add((observation,
                           rdflib.RDF.type,
                           self._uri_ref('', 'Stepping')))
                graph.add((observation,
                           self._uri_ref('', 'has_pedestrian'),
                           self._uri_ref('Id-User', event.source_id)))
                graph.add((observation,
                           self._uri_ref('', 'stepping_date'),
                           rdflib.Literal(_to_xsd_date(data['dateTime']),
                                          datatype=XSD.date)))
                for steps_data in data['stepsList']:
                    steps = rdflib.BNode()
                    graph.add((steps,
                               rdflib.RDF.type,
                               self._uri_ref('', 'Step_Set')))
                    graph.add((steps,
                               self._uri_ref('', 'stepping_time'),
                               rdflib.Literal(steps_data['timeLog'],
                                              datatype=XSD.time)))
                    graph.add((steps,
                               self._uri_ref('', 'steps'),
                               rdflib.Literal(steps_data['steps'])))
                    graph.add((observation,
                               self._uri_ref('', 'has_step_set'),
                               steps))
        except KeyError:
            ## import traceback
            ## print(traceback.format_exc())
            return [event]
        else:
            return [self._create_event(event, graph)]

    def annotate_event_heart_rate(self, event):
        if (not isinstance(event, ztreamy.events.JSONEvent)
            or len(event.body.keys()) != 1):
            return [event]
        try:
            dataset = event.body['dataset']
            graph = self._create_graph()
            for i, data in enumerate(dataset):
                observation = self._uri_ref('Id-Observation',
                                            '{}-{}'.format(event.event_id, i))
                graph.add((observation,
                           rdflib.RDF.type,
                           self._uri_ref('', 'Heart_Frequency')))
                graph.add((observation,
                           self._uri_ref('', 'has_pedestrian'),
                           self._uri_ref('Id-User', event.source_id)))
                graph.add((observation,
                           self._uri_ref('', 'heart_date'),
                           rdflib.Literal(_to_xsd_date(data['dateTime']),
                                          datatype=XSD.date)))
                for heart_data in data['heartRateList']:
                    heart = rdflib.BNode()
                    graph.add((heart,
                               rdflib.RDF.type,
                               self._uri_ref('', 'Heart_Set')))
                    graph.add((heart,
                               self._uri_ref('', 'heart_time'),
                               rdflib.Literal(heart_data['timeLog'],
                                              datatype=XSD.time)))
                    graph.add((heart,
                               self._uri_ref('', 'bpm'),
                               rdflib.Literal(heart_data['heartRate'])))
                    graph.add((observation,
                               self._uri_ref('', 'has_heart_set'),
                               heart))
        except KeyError:
            ## import traceback
            ## print(traceback.format_exc())
            return [event]
        else:
            return [self._create_event(event, graph)]

    def annotate_event_sleep(self, event):
        if (not isinstance(event, ztreamy.events.JSONEvent)
            or len(event.body.keys()) != 1):
            return [event]
        try:
            dataset = event.body['dataset']
            graph = self._create_graph()
            for i, data in enumerate(dataset):
                observation = self._uri_ref('Id-Observation',
                                            '{}-{}'.format(event.event_id, i))
                graph.add((observation,
                           rdflib.RDF.type,
                           self._uri_ref('', 'Sleep')))
                graph.add((observation,
                           self._uri_ref('', 'has_user'),
                           self._uri_ref('Id-User', event.source_id)))
                graph.add((observation,
                           self._uri_ref('', 'awakenings'),
                           rdflib.Literal(data['awakenings'])))
                graph.add((observation,
                           self._uri_ref('', 'minutes_asleep'),
                           rdflib.Literal(data['minutesAsleep'])))
                graph.add((observation,
                           self._uri_ref('', 'minutes_in_bed'),
                           rdflib.Literal(data['minutesInBed'])))
                graph.add((observation,
                           self._uri_ref('', 'sleeping_date'),
                           rdflib.Literal(_to_xsd_date(data['dateTime']),
                                          datatype=XSD.date)))
                graph.add((observation,
                           self._uri_ref('', 'sleeping_start_time'),
                           rdflib.Literal(data['startTime'],
                                          datatype=XSD.time)))
                graph.add((observation,
                           self._uri_ref('', 'sleeping_end_time'),
                           rdflib.Literal(data['endTime'],
                                          datatype=XSD.time)))
        except KeyError:
            ## import traceback
            ## print(traceback.format_exc())
            return [event]
        else:
            return [self._create_event(event, graph)]


class AnnotatedStream(ztreamy.server.Stream):
    def __init__(self, path, annotator, **kwargs):
        kwargs['event_adapter'] = annotator.annotate_events
        kwargs['parse_event_body'] = True
        super(AnnotatedStream, self).__init__(path, **kwargs)


class AnnotatedRelayStream(ztreamy.server.RelayStream):
    def __init__(self, path, streams, annotator, **kwargs):
        kwargs['event_adapter'] = annotator.annotate_events
        kwargs['parse_event_body'] = True
        super(AnnotatedRelayStream, self).__init__(path, streams, **kwargs)


def _to_xsd_date(date):
    """Converts a date DD/MM/YYYY to xsd:date (YYYY-MM-DD)."""
    if _re_hermes_date.match(date):
        return _re_hermes_date.sub(r'\3-\2-\1', date)
    else:
        raise ValueError('Wrong date format')

_re_hermes_date = re.compile(r'^(\d\d)/(\d\d)/(\d\d\d\d)$')
