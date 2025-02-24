/*
 The MIT License

 Copyright (c) 2004-2011 Paul R. Holser, Jr.

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
/*
 * Contains changes for GemFireXD distributed data platform
 * (some marked by "GemFire Addition" blocks).
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. licensed under
 * the same license as the original file. All rights reserved.
 */

package joptsimple;

import java.util.Collection;

/**
 * Thrown when the option parser discovers an option that requires an argument, but that argument is missing.
 *
 * @author <a href="mailto:pholser@alumni.rice.edu">Paul Holser</a>
 * @author Nikhil Jadhav
 */
public class OptionMissingRequiredArgumentException extends OptionException {
    private static final long serialVersionUID = -1L;

    OptionMissingRequiredArgumentException( Collection<String> options ) {
        super( options );
    }

    // GemFire Addition: Added to include OptionSet
    OptionMissingRequiredArgumentException( Collection<String> options, OptionSet detected ) {
        super( options, detected );
    }
    
    @Override
    public String getMessage() {
        return "Option " + multipleOptionMessage() + " requires an argument";
    }
}
