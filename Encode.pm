package Kafka::Encode;

use strict;
use Digest::CRC qw(crc32);
use Carp;

our $CURRENT_MAGIC_VALUE = 1;

=item encode_message(message => ..., compression => ...)

encodes a message for Kafka

=cut

sub encode_message {
    my %args = @_;
    croak "no message passed" if ( ! $args{'message'} );
    return pack('CCN', $CURRENT_MAGIC_VALUE, $args{'compression'}, crc32($args{'message'})) . $args{'message'};
}

=item encode_produce_request(request_id => ..., topic => ..., partition => ..., messages => [...], compression => ...)

encodes a set of messages from a producer.  messages should be an array ref of messages to send

=cut

sub encode_produce_request {
    my %args = @_;
    my $message_set = '';

    foreach my $message ( @{$args{'messages'}} ) {
        my $encoded = encode_message(message => $message, compression => $args{'compression'});
        $message_set = pack('N', length($encoded)) . $encoded;
    }

    my $request =   pack('n', $args{'request_id'}) . 
                    pack('n', length($args{'topic'})) . $args{'topic'} .
                    pack('N', $args{'partition'}) .
                    pack('N', length($message_set)) . $message_set;

    return pack('N', length($request)) . $request;
}

1;
