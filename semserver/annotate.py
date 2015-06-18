from __future__ import unicode_literals, print_function

import rdflib
from rdflib.namespace import NamespaceManager

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
            ('', 'High_Heart_Rate'),
            ('', 'Stopping'),
            ('', 'Kinetics'),
            ('', 'Acceleration'),
            ('', 'Stepping'),
            ('', 'Step_Set'),
            ('', 'has_user'),
            ('', 'has_driver'),
            ('', 'has_pedestrian'),
            ('', 'orientation'),
            ('', 'has_location'),
            ('', 'completed_distance'),
            ('', 'speed_value'),
            ('', 'speed_type'),
            ('', 'bpm'),
            ('', 'stops'),
            ('', 'energy'),
            ('', 'acceleration'),
            ('', 'on_date'),
            ('', 'at_time'),
            ('', 'steps'),
            ('', 'has_step_set'),
            ('geo', 'SpatialThing'),
            ('geo', 'lat'),
            ('geo', 'long'),
        ])
        self.annotation_dispatcher.update({
            HermesAnnotator.application_id_driver: self.annotate_event_driver,
            HermesAnnotator.application_id_steps: self.annotate_event_steps,
        })
        self.classes_driver = {
            'Average Speed Section': 'Speed',
            'Standard Deviation of Vehicle Speed Section': 'Speed',
            'Inefficient Speed Section': 'Speed',
            'Heart Rate Section': 'Heart_Rate',
            'Standard Deviation Heart Rate Section': 'Heart_Rate',
            'High Heart Rate': 'High_Heart_Rate',
            'Stops Section': 'Stopping',
            'Positive Kinetic Energy': 'Kinetics',
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
                       rdflib.Literal(data['distancia'])))
        if top_key == 'Average Speed Section':
            graph.add((observation,
                       self._uri_ref('', 'speed_value'),
                       rdflib.Literal(data['value'])))
            graph.add((observation,
                       self._uri_ref('', 'speed_type'),
                       rdflib.Literal('average')))
        elif top_key == 'Standard Deviation of Vehicle Speed Section':
            graph.add((observation,
                       self._uri_ref('', 'speed_value'),
                       rdflib.Literal(data['value'])))
            graph.add((observation,
                       self._uri_ref('', 'speed_type'),
                       rdflib.Literal('deviation')))
        elif top_key == 'Inefficient Speed Section':
            graph.add((observation,
                       self._uri_ref('', 'speed_type'),
                       rdflib.Literal(data['value'])))
        elif (top_key == 'Heart Rate Section'
              or top_key == 'High Heart Rate'):
            graph.add((observation,
                       self._uri_ref('', 'bpm'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Standard Deviation Heart Rate Section':
            graph.add((observation,
                       self._uri_ref('', 'bpm'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Stops Section':
            graph.add((observation,
                       self._uri_ref('', 'stops'),
                       rdflib.Literal(data['value'])))
        elif top_key == 'Positive Kinetic Energy':
            graph.add((observation,
                       self._uri_ref('', 'energy'),
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
                           self._uri_ref('', 'on_date'),
                           rdflib.Literal(data['dateTime'])))
                for steps_data in data['stepsList']:
                    steps = rdflib.BNode()
                    graph.add((steps,
                               self._uri_ref('', 'at_time'),
                               rdflib.Literal(steps_data['timeLog'])))
                    graph.add((steps,
                               self._uri_ref('', 'steps'),
                               rdflib.Literal(steps_data['steps'])))
                    graph.add((observation,
                               self._uri_ref('', 'has_step_set'),
                               steps))
        except KeyError:
            import traceback
            print(traceback.format_exc())
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
