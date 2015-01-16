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

    def annotate_events(self, events):
        annotated = []
        for event in events:
            annotated.extend(self.annotate_event(event))
        return annotated

    def annotate_event(self, event):
        """Annotates the event and returns a resulting list of events.

        Intended to be overriden by subclasses.

        """
        return event

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


class DriverAnnotator(GenericAnnotator):

    ns_hermes = 'http://webtlab.it.uc3m.es/ns/hermes/driver#'
    ns_geo = 'http://www.w3.org/2003/01/geo/wgs84_pos#'
    application_id = 'smart-driver'

    def __init__(self):
        super(DriverAnnotator, self).__init__()
        self.register_ns('', DriverAnnotator.ns_hermes, prefix='hermes')
        self.register_ns('geo', DriverAnnotator.ns_geo, prefix='geo')
        self._create_uri_refs([
            ('', 'Observation'),
            ('', 'Driver'),
            ('', 'ObservationId'),
            ('', 'DriverId'),
            ('', 'average_hrm'),
            ('', 'average_speed'),
            ('', 'efficiency'),
            ('', 'work_load'),
            ('', 'driver'),
            ('geo', 'lat'),
            ('geo', 'long'),
        ])

    def annotate_event(self, event):
        if (not isinstance(event, ztreamy.events.JSONEvent)
            or not event.application_id == DriverAnnotator.application_id):
            return []
        graph = self._create_graph()
        observation = self._uri_ref('ObservationId', event.event_id)
        graph.add((observation,
                   rdflib.RDF.type,
                   self._uri_ref('', 'Observation')))
        graph.add((observation,
                   self._uri_ref('', 'driver'),
                   self._uri_ref('DriverId', event.source_id)))
        if 'averageHRM' in event.body:
            graph.add((observation,
                       self._uri_ref('', 'average_hrm'),
                       rdflib.Literal(event.body['averageHRM'])))
        if 'averageSpeed' in event.body:
            graph.add((observation,
                       self._uri_ref('', 'average_speed'),
                       rdflib.Literal(event.body['averageSpeed'])))
        if 'efficiency' in event.body:
            graph.add((observation,
                       self._uri_ref('', 'efficiency'),
                       rdflib.Literal(event.body['efficiency'].strip())))
        if 'workLoad' in event.body:
            graph.add((observation,
                       self._uri_ref('', 'workload'),
                       rdflib.Literal(event.body['workLoad'].strip())))
        if 'longitud' in event.body:
            graph.add((observation,
                       self._uri_ref('geo', 'long'),
                       rdflib.Literal(event.body['longitud'])))
        if 'latitud' in event.body:
            graph.add((observation,
                       self._uri_ref('geo', 'lat'),
                       rdflib.Literal(event.body['latitud'])))
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
